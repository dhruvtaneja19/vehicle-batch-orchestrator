from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.logger import get_logger

logger = get_logger(__name__)


def upload_file(
    bucket_name: str,
    local_file: Path,
    s3_key: str,
):
    hook = S3Hook(
        aws_conn_id="aws_default"
    )

    logger.info(
        f"Uploading {local_file} → s3://{bucket_name}/{s3_key}"
    )

    hook.load_file(
        filename=str(local_file),
        bucket_name=bucket_name,
        key=s3_key,
        replace=True,
    )

    logger.info("Upload completed")