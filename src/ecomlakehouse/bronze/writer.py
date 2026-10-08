from io import BytesIO
from datetime import datetime, timezone

import pandas as pd

from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def write_dataframe_to_bronze(
    df: pd.DataFrame,
    table_name: str,
) -> str:
    """
    Write a DataFrame to MinIO Bronze as a Parquet object.
    """

    if df.empty:
        raise ValueError(f"No data found for table: {table_name}")

    now = datetime.now(timezone.utc)

    ingestion_date = now.strftime("%Y-%m-%d")
    ingestion_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    object_key = (
        f"bronze/"
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
