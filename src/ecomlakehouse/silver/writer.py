import os
import shutil
import tempfile
from datetime import UTC, datetime
from io import BytesIO

import pandas as pd
from pyspark.sql import DataFrame

from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def write_df_to_silver(
    df: DataFrame,
    table_name: str,
    run_id: str | None = None,
) -> list[str]:
    """
    Write a Spark DataFrame directly to MinIO Silver as Parquet objects.
    Streams via local temp directory and uploads via Boto3, completely avoiding
    Python memory bottlenecks (no toPandas).
    Returns list of committed S3 object keys.
    """
    now = datetime.now(UTC)
    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    temp_dir = tempfile.mkdtemp(prefix=f"silver_{table_name}_")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(temp_dir)

        parquet_files = [
            f
            for f in os.listdir(temp_dir)
            if f.endswith(".parquet") and not f.startswith(".") and not f.startswith("_")
        ]

        s3 = get_s3_client()
        bucket = get_bucket_name()
        committed_keys = []

        for idx, pf in enumerate(sorted(parquet_files)):
            local_path = os.path.join(temp_dir, pf)
            s3_key = (
                f"silver/"
                f"{table_name}/"
                f"ingestion_date={ingestion_date}/"
                f"{table_name}_{ingestion_timestamp}_{idx:04d}.parquet"
            )
            s3.upload_file(local_path, bucket, s3_key)
            print(f"Written Silver: s3://{bucket}/{s3_key}")
            committed_keys.append(s3_key)

        return committed_keys
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def write_quarantine_to_silver(
    df: DataFrame,
    table_name: str,
    run_id: str | None = None,
) -> list[str]:
    """
    Write quarantined Spark DataFrame directly to MinIO Silver quarantine path.
    """
    now = datetime.now(UTC)
    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    temp_dir = tempfile.mkdtemp(prefix=f"quarantine_{table_name}_")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(temp_dir)

        parquet_files = [
            f
            for f in os.listdir(temp_dir)
            if f.endswith(".parquet") and not f.startswith(".") and not f.startswith("_")
        ]

        s3 = get_s3_client()
        bucket = get_bucket_name()
        committed_keys = []

        for idx, pf in enumerate(sorted(parquet_files)):
            local_path = os.path.join(temp_dir, pf)
            s3_key = (
                f"silver/_quarantine/"
                f"{table_name}/"
                f"ingestion_date={ingestion_date}/"
                f"{table_name}_quarantine_{ingestion_timestamp}_{idx:04d}.parquet"
            )
            s3.upload_file(local_path, bucket, s3_key)
            print(f"Quarantine written: s3://{bucket}/{s3_key}")
            committed_keys.append(s3_key)

        return committed_keys
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
