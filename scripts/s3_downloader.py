from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.logger import get_logger

logger = get_logger(__name__)


def download_csv(
    bucket_name: str,
    s3_key: str,
    local_file: Path,
):

    hook = S3Hook(
        aws_conn_id="aws_default"
    )

    logger.info(
        f"Downloading s3://{bucket_name}/{s3_key}"
    )

    obj = hook.get_key(
        key=s3_key,
        bucket_name=bucket_name,
    )

    local_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(local_file, "wb") as f:
        obj.download_fileobj(f)

    logger.info(
        f"Downloaded to {local_file}"
    )