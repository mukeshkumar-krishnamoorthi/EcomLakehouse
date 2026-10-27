from datetime import UTC, datetime
from io import BytesIO

import pandas as pd

from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def write_dataframe_to_silver(
    df: pd.DataFrame,
    table_name: str,
) -> str:
    """
    Write a valid DataFrame to MinIO Silver as a Parquet object.
    Path: silver/{table_name}/ingestion_date=YYYY-MM-DD/{table_name}_{timestamp}.parquet
    """

    if df.empty:
        raise ValueError(f"No data found for table: {table_name}")

    now = datetime.now(UTC)

    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    object_key = (
        f"silver/"
        f"{table_name}/"
        f"ingestion_date={ingestion_date}/"
        f"{table_name}_{ingestion_timestamp}.parquet"
    )

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    buffer.seek(0)

    s3 = get_s3_client()

    s3.put_object(
        Bucket=get_bucket_name(),
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Written: s3://{get_bucket_name()}/{object_key}")

    return object_key


def write_quarantine_to_silver(
    df: pd.DataFrame,
    table_name: str,
) -> str:
    """
    Write quarantined records to MinIO Silver quarantine path as a Parquet object.
    Path: silver/_quarantine/{table_name}/ingestion_date=YYYY-MM-DD/
          {table_name}_quarantine_{timestamp}.parquet
    """
    if df.empty:
        return ""

    now = datetime.now(UTC)

    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    object_key = (
        f"silver/_quarantine/"
        f"{table_name}/"
        f"ingestion_date={ingestion_date}/"
        f"{table_name}_quarantine_{ingestion_timestamp}.parquet"
    )

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
    )

    buffer.seek(0)

    s3 = get_s3_client()

    s3.put_object(
        Bucket=get_bucket_name(),
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"Quarantine written: s3://{get_bucket_name()}/{object_key}")

    return object_key
