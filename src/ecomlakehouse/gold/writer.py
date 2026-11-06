import shutil
import os
import tempfile
from datetime import datetime, UTC

from pyspark.sql import DataFrame

from ecomlakehouse.utils.storage import get_bucket_name
from ecomlakehouse.utils.storage import get_s3_client


def write_gold_table(
    df: DataFrame,
    table_name: str
) -> list[str]:
    """
    Write a Gold Spark DataFrame to MinIO S3 as partitioned/staged Parquet objects.
    Streams via a local temp directory to avoid Python driver memory bottlenecks.
    """

    if df.rdd.isEmpty():
        raise ValueError(f"No data found for table: {table_name}")

    now = datetime.now(UTC)

    ingested_date = now.strftime("%Y-%m-%d")
    ingested_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    temp_dir = tempfile.mkdtemp(prefix=f"gold_{table_name}_")
    try:
        df.coalesce(1).write.mode('overwrite').parquet(temp_dir)

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
                f"gold/"
                f"{table_name}/"
                f"ingested_date={ingested_date}/"
                f"{table_name}_{ingested_timestamp}_{idx:04d}.parquet"
            )

            s3.upload_file(local_path, bucket, s3_key)
            print(f"Written Gold: s3://{bucket}/{s3_key}")
            committed_keys.append(s3_key)

        return committed_keys
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
