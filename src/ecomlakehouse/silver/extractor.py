import os
import tempfile

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)

DEFAULT_MAX_FILES_PER_BATCH = int(
    os.getenv("DEFAULT_MAX_FILES_PER_BATCH", "20"))


def extract_data_from_bronze(
    spark: SparkSession,
    table_name: str,
    last_processed_key: str | None = None,
    max_files: int = DEFAULT_MAX_FILES_PER_BATCH,
) -> tuple[DataFrame | None, str | None, bool, str | None]:
    """
    Download up to `max_files` Bronze Parquet files newer than the supplied key
    to a local cache directory and return a Spark DataFrame directly.
    Completely avoids converting to Pandas or Py4J serialization in Python memory.

    Returns:
        (Spark DataFrame | None, latest_processed_key, has_more_files, cache_dir)
    """
    s3 = get_s3_client()
    bucket = get_bucket_name()
    prefix = f"bronze/{table_name}/"

    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))

    parquet_objects = sorted(
        (obj for obj in objects if obj["Key"].endswith(".parquet")),
        key=lambda obj: obj["Key"],
    )

    if isinstance(last_processed_key, dict):
        last_processed_key = (
            last_processed_key.get("watermark_end")
            or last_processed_key.get("last_processed_key")
        )

    if last_processed_key is not None:
        parquet_objects = [
            obj for obj in parquet_objects if obj["Key"] > str(last_processed_key)
        ]

    if not parquet_objects:
        return None, last_processed_key, False, None

    has_more = len(parquet_objects) > max_files
    batch_objects = parquet_objects[:max_files]
    latest_key = batch_objects[-1]["Key"]

    cache_dir = tempfile.mkdtemp(prefix=f"bronze_cache_{table_name}_")

    downloaded_files = []
    key_map = {}
    for obj in batch_objects:
        fname = os.path.basename(obj["Key"])
        local_path = os.path.join(cache_dir, fname)
        s3.download_file(bucket, obj["Key"], local_path)
        downloaded_files.append(local_path)
        key_map[fname] = obj["Key"]

    # Read native Parquet files via Spark JVM
    spark_df = spark.read.parquet(cache_dir)

    # Preserve _source_file lineage
    if len(batch_objects) == 1:
        spark_df = spark_df.withColumn(
            "_source_file", F.lit(batch_objects[0]["Key"]))
    else:
        mapping_cases = [
            F.when(F.input_file_name().endswith(fname), F.lit(s3_key))
            for fname, s3_key in key_map.items()
        ]
        spark_df = spark_df.withColumn(
            "_source_file",
            F.coalesce(*mapping_cases, F.input_file_name()),
        )

    return spark_df, latest_key, has_more, cache_dir
