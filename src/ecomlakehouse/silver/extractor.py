
from io import BytesIO

import pandas as pd

from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def extract_data_from_bronze(
    table_name: str,
    last_processed_key: str | None = None,
) -> tuple[pd.DataFrame, str | None]:
    """
    Read Bronze Parquet files newer than the supplied key.

    Returns:
        (DataFrame containing extracted rows, latest processed key)
    """
    s3 = get_s3_client()
    bucket = get_bucket_name()
    prefix = f"bronze/{table_name}/"

    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        objects.extend(page.get("Contents", []))

    parquet_objects = sorted(
        (
            obj for obj in objects
            if obj["Key"].endswith(".parquet")
        ),
        key=lambda obj: obj["Key"],
    )

    if last_processed_key is not None:
        parquet_objects = [
            obj for obj in parquet_objects
            if obj["Key"] > last_processed_key
        ]

    if not parquet_objects:
        return pd.DataFrame(), last_processed_key

    dataframes = []

    for obj in parquet_objects:
        object_key = obj["Key"]

        response = s3.get_object(
            Bucket=bucket,
            Key=object_key,
        )

        parquet_bytes = response["Body"].read()

        if not parquet_bytes:
            raise ValueError(
                f"Empty Parquet object: {object_key}"
            )

        df = pd.read_parquet(BytesIO(parquet_bytes))
        df["_source_file"] = object_key
        dataframes.append(df)

    result = pd.concat(
        dataframes,
        ignore_index=True,
    )

    latest_key = parquet_objects[-1]["Key"]

    return result, latest_key
