from datetime import timedelta

import pendulum
from pendulum import parse
from sqlalchemy.engine import Engine

from ecomlakehouse.bronze.writer import write_dataframe_to_bronze
from ecomlakehouse.ingestion.postgres import extract_incremental
from ecomlakehouse.utils.logging_utils import get_logger

logger = get_logger(__name__)


SOURCE_TABLES = [
    "categories",
    "products",
    "customers",
    "stores",
    "orders",
    "order_items",
    "payments",
]

PIPELINE_NAME = "postgres_to_bronze"


def run_incremental_ingestion(
    engine: Engine,
    **kwargs,
) -> None:
    """
    Run incremental ingestion from PostgreSQL to the bronze layer.
    """

    data_interval_start = kwargs.get("data_interval_start")
    data_interval_end = kwargs.get("data_interval_end")

    previous_success_end = kwargs.get("prev_data_interval_end_success")
    previous_success_start = kwargs.get("prev_data_interval_start_success")

    logger.info("\n")
    logger.info(
        "Starting pipeline: %s",
        PIPELINE_NAME,
    )

    logger.info(
        "Current interval: %s -> %s",
        data_interval_start,
        data_interval_end,
    )

    def to_datetime(val):
        if val is None or str(val).strip().lower() in ("none", "", "null"):
            return None
        try:
            return parse(str(val))
        except Exception:
            return None

    data_interval_end = to_datetime(data_interval_end)
    if not data_interval_end:

        data_interval_end = pendulum.now("UTC")

    prev_end = to_datetime(previous_success_end)
    if prev_end:
        logger.info(
            "Previous successful interval found: %s -> %s",
            previous_success_start,
            prev_end,
        )
        previous_success_end = prev_end
    else:
        start_dt = to_datetime(data_interval_start)
        if start_dt:
            logger.info(
                "No previous successful run found. "
                "Using current interval start as extraction start: %s",
                start_dt,
            )
            previous_success_end = start_dt
        else:
            logger.info(
                "No previous run or interval start found. Defaulting to 30 mins prior to end."
            )
            previous_success_end = data_interval_end - timedelta(minutes=30)

    # Fallback if window start >= end (e.g. manual triggers or initial runs)
    if previous_success_end and data_interval_end and previous_success_end >= data_interval_end:
        logger.warning(
            "Extraction start (%s) >= extraction end (%s). "
            "Defaulting extraction window to 30 minutes prior to end.",
            previous_success_end,
            data_interval_end,
        )
        previous_success_end = data_interval_end - timedelta(minutes=30)

    logger.info(
        "Incremental extraction window: %s -> %s",
        previous_success_end,
        data_interval_end,
    )

    for table_name in SOURCE_TABLES:

        logger.info("\n")

        try:

            logger.info(
                "Starting extraction for table: %s",
                table_name,
            )

            df = extract_incremental(
                engine=engine,
                table_name=table_name,
                start_time=previous_success_end,
                end_time=data_interval_end,
            )

            logger.info(
                "Extraction completed for table: %s | rows=%d",
                table_name,
                len(df),
            )

            if df.empty:
                logger.info(
                    "No changes found for table: %s | "
                    "window=%s -> %s",
                    table_name,
                    previous_success_end,
                    data_interval_end,
                )

                continue

            logger.info(
                "Writing data to bronze layer | "
                "table=%s | rows=%d",
                table_name,
                len(df),
            )

            write_dataframe_to_bronze(
                df=df,
                table_name=table_name,
            )

            logger.info(
                "Bronze write completed successfully | "
                "table=%s | rows=%d",
                table_name,
                len(df),
            )

        except Exception:
            logger.exception(
                "Failed to process table: %s",
                table_name,
            )

            raise

    logger.info(
        "Pipeline completed successfully: %s",
        PIPELINE_NAME,
    )
