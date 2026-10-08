from sqlalchemy.engine import Engine
import logging

from ecomlakehouse.utils.storage import get_s3_client
from ecomlakehouse.ingestion.postgres import extract_table
from ecomlakehouse.bronze.writer import write_dataframe_to_bronze


logger = logging.getLogger(__name__)


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

    logger.info(
        "Bronze cleanup started | bucket=%s | prefix=%s",
        BRONZE_BUCKET,
        BRONZE_PREFIX,
    )

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

        logger.info(
            "Bronze objects deleted | batch_count=%d | total_deleted=%d",
            len(delete_objects),
            total_deleted,
        )

    logger.info(
        "Bronze cleanup completed | deleted_objects=%d",
        total_deleted,
    )


def run_batch_ingestion(engine: Engine) -> None:
    """
    Run batch ingestion for all source tables.
    """

    logger.info(
        "Initial batch ingestion started | tables=%d",
        len(SOURCE_TABLES),
    )

    clear_bronze()

    for table_name in SOURCE_TABLES:

        logger.info(
            "Table ingestion started | table=%s",
            table_name,
        )

        try:
            logger.info(
                "Extraction started | table=%s",
                table_name,
            )

            df = extract_table(
                engine=engine,
                table_name=table_name,
            )

            logger.info(
                "Extraction completed | "
                "table=%s | rows=%d",
                table_name,
                len(df),
            )

            logger.info(
                "Bronze write started | "
                "table=%s | rows=%d",
                table_name,
                len(df),
            )

            object_key = write_dataframe_to_bronze(
                df=df,
                table_name=table_name,
            )

            logger.info(
                "Bronze write completed | "
                "table=%s | rows=%d | object_key=%s",
                table_name,
                len(df),
                object_key,
            )

            logger.info(
                "Table ingestion completed | table=%s",
                table_name,
            )

        except Exception:
            logger.exception(
                "Table ingestion failed | table=%s",
                table_name,
            )
            raise

    logger.info(
        "Initial batch ingestion completed | tables=%d",
        len(SOURCE_TABLES),
    )
