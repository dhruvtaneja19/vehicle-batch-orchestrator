from pathlib import Path

import pandas as pd
from airflow.models import Variable

from utils.logger import get_logger
from scripts.enrich_vehicle import enrich_vehicle
from utils.metrics import batch_summary

logger = get_logger(__name__)


def process_batch(batch_file: Path, environment: str) -> pd.DataFrame:
    logger.info(f"Processing {batch_file.name}")

    df = pd.read_csv(batch_file)

    logger.info(f"Rows = {len(df)}")

    enriched_rows = []
    failed = []

    enable_valuation = (
        Variable.get(
            "ENABLE_VALUATION",
            default_var="true"
        ).lower() == "true"
    )

    logger.info(f"Valuation Enabled = {enable_valuation}")

    for _, row in df.iterrows():
        vehicle = row.to_dict()
        try:
            enriched_rows.append(
                enrich_vehicle(vehicle, environment, enable_valuation)
            )
        except Exception as e:
            logger.error(
                f"{vehicle['vehicle_id']} failed: {e}"
            )
            failed.append(vehicle)

    if failed:
        failed_dir = batch_file.parent.parent / "failed"
        failed_dir.mkdir(parents=True, exist_ok=True)
        failed_df = pd.DataFrame(failed)
        failed_df.to_csv(failed_dir / f"failed_{batch_file.name}", index=False)
        
    summary = batch_summary(len(df), len(enriched_rows), len(failed))
    success_rate = (summary['success']/summary['total'])*100 if summary['total'] > 0 else 0
    logger.info(
        "\n==========================\n"
        "Batch Summary\n"
        "==========================\n"
        f"Total Records : {summary['total']}\n"
        f"Success : {summary['success']}\n"
        f"Failed : {summary['failed']}\n"
        f"Success Rate : {success_rate:.1f}%\n"
        "=========================="
    )

    return pd.DataFrame(enriched_rows)