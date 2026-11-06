import os
import tempfile

from pyspark.sql import SparkSession, DataFrame

from ecomlakehouse.utils.storage import get_bucket_name
from ecomlakehouse.utils.storage import get_s3_client


def load_silver_table(
    spark: SparkSession,
    table_name: str
) -> tuple[DataFrame | None, str | None]:
    """
    Load all valid Silver Parquet files for a table from MinIO into a Spark DataFrame.
    Returns (spark_df, cache_dir).
    """

    s3 = get_s3_client()
    bucket = get_bucket_name()
    prefix = f"silver/{table_name}/"

    paginator = s3.get_paginator('list_objects_v2')
    objects = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".parquet") and "_quarantine" not in key:
                objects.append(key)

    if not objects:
        return None, None

    cache_dir = tempfile.mkdtemp(prefix=f"silver_cache_{table_name}_")

    for key in objects:
        fname = os.path.basename(key)
        local_path = os.path.join(cache_dir, fname)
        s3.download_file(bucket, key, local_path)

    spark_df = spark.read.parquet(cache_dir)
    return spark_df, cache_dir
