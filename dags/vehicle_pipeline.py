import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta

from validators.vehicle_validator import validate_dataframe
# pyrefly: ignore [missing-import]
from airflow.sdk import dag, task, Variable

#from airflow.sd import Variable

from configs.pipeline_config import DEFAULT_INPUT_FILE, DEFAULT_CHUNK_SIZE, OUTPUT_FOLDER, S3_BUCKET, S3_INPUT_KEY, S3_OUTPUT_KEY, S3_FAILED_KEY
from pathlib import Path

from scripts.csv_reader import read_csv
from scripts.save_chunks import save_chunks
from scripts.process_batch import process_batch
from utils.logger import get_logger
from utils.dataframe_cleaner import clean_dataframe
from utils.chunker import split_into_chunks


logger = get_logger(__name__)


@dag(
    dag_id="vehicle_batch_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["vehicle", "batch"],
)

def vehicle_pipeline():

    @task
    def load_vehicle_data():
        from scripts.check_pipeline_state import check_pipeline_state
        check_pipeline_state()
        
        from scripts.s3_downloader import download_csv
        download_csv(
            bucket_name=S3_BUCKET,
            s3_key=S3_INPUT_KEY,
            local_file=DEFAULT_INPUT_FILE,
        )
        
        df = read_csv(DEFAULT_INPUT_FILE)
        validate_dataframe(df)
        df = clean_dataframe(df)
        return df.to_dict("records")

    @task
    def split_batches(records):
        import pandas as pd
        df = pd.DataFrame(records)
        chunk_size = int(
            Variable.get(
                "CHUNK_SIZE",
                default_var=DEFAULT_CHUNK_SIZE
            )
        )

        logger.info(f"Chunk Size = {chunk_size}")

        batches = split_into_chunks(df, chunk_size)
        batch_files = save_chunks(batches, OUTPUT_FOLDER)
        return [str(f) for f in batch_files]

    @task(
        retries=2,
        retry_delay=timedelta(seconds=30),
        pool="vehicle_api_pool",
    )
    def process_batch_task(batch_file):
        from airflow.sdk import get_current_context
        
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        environment = conf.get("environment", "staging")
        
        return process_batch(Path(batch_file), environment)

    @task
    def merge_batches(results):
        import pandas as pd
        from scripts.failed_batches import get_failed_records
        final = pd.concat(results)
        final.to_csv(
            OUTPUT_FOLDER / "final_vehicle_report.csv",
            index=False,
        )
        
        from scripts.s3_uploader import upload_file

        report = OUTPUT_FOLDER / "final_vehicle_report.csv"

        upload_file(
            bucket_name=S3_BUCKET,
            local_file=report,
            s3_key=S3_OUTPUT_KEY,
        )
        
        failures = get_failed_records()
        if failures:
            pd.DataFrame(failures).to_csv(
                OUTPUT_FOLDER / "failed_records.csv",
                index=False
            )
            
            upload_file(
                bucket_name=S3_BUCKET,
                local_file=OUTPUT_FOLDER / "failed_records.csv",
                s3_key=S3_FAILED_KEY,
            )
            
        return len(final)

    @task
    def pipeline_completed(total):
        logger.info(f"Pipeline completed. Rows={total}")

    records = load_vehicle_data()
    batch_files = split_batches(records)
    
    processed = process_batch_task.expand(
        batch_file=batch_files
    )
    
    total = merge_batches(processed)
    pipeline_completed(total)

vehicle_pipeline()