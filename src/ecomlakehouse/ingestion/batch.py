from sqlalchemy.engine import Engine

from ecomlakehouse.bronze.writer import write_dataframe_to_bronze
from ecomlakehouse.ingestion.postgres import extract_table

from ecomlakehouse.utils.storage import get_s3_client


SOURCE_TABLES = [
    "categories",
    "products",
    "customers",
    "stores",
    "payment_methods",
    "orders",
    "order_items",
    "payments",
]


BRONZE_BUCKET = "ecommerce-lake"
BRONZE_PREFIX = "bronze/"


def clear_bronze() -> None:
    """
    Delete all objects under the Bronze S3 prefix.
    """
    s3 = get_s3_client()

    print(f"Clearing s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}")

    paginator = s3.get_paginator("list_objects_v2")

    total_deleted = 0

    for page in paginator.paginate(
        Bucket=BRONZE_BUCKET,
        Prefix=BRONZE_PREFIX,
    ):
        objects = page.get("Contents", [])

        if not objects:
            continue

        delete_objects = [
            {"Key": obj["Key"]}
            for obj in objects
        ]

        s3.delete_objects(
            Bucket=BRONZE_BUCKET,
            Delete={
                "Objects": delete_objects,
            },
        )

        total_deleted += len(delete_objects)

    print(f"Bronze cleared. Deleted {total_deleted} objects.")


def run_batch_ingestion(engine: Engine) -> None:
    """
    Run batch ingestion for all source tables.
    """

    clear_bronze()

    for table_name in SOURCE_TABLES:

        print(f"\nExtracting: {table_name}")

        df = extract_table(
            engine=engine,
            table_name=table_name,
        )

        print(f"Rows extracted: {len(df)}")

        write_dataframe_to_bronze(
            df=df,
            table_name=table_name,
        )
