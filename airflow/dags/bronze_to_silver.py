from datetime import UTC, datetime

from airflow.sdk import Asset, dag, task

from ecomlakehouse.constents.tables import SOURCE_TABLES
from ecomlakehouse.ingestion.checkpoint import (
    get_checkpoint,
    save_checkpoint,
)
from ecomlakehouse.quality.validator import DataQualityValidator
from ecomlakehouse.silver.extractor import extract_data_from_bronze
from ecomlakehouse.silver.writer import (
    write_dataframe_to_silver,
    write_quarantine_to_silver,
)
from ecomlakehouse.utils.logging_utils import get_logger
from ecomlakehouse.utils.spark import get_spark

logger = get_logger(__name__)

CHECKPOINT = "BRONZE_TO_SILVER"

BRONZE_INGESTION_ASSET = Asset(
    "ecomlakehouse://bronze/ingestion_completed"
)


@dag(
    dag_id="incremental_bronze_to_silver",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    schedule=[BRONZE_INGESTION_ASSET],
    catchup=False,
    tags=["Bronze", "Silver", "Asset"],
)
def incremental_bronze_to_silver():

    @task
    def ingest_table(table_name: str) -> dict:
        # 1. Load checkpoint
        last_key = get_checkpoint(
            checkpoint=CHECKPOINT,
            table_name=table_name
        )

        # 2. Extract new Bronze files
        pandas_df, latest_key = extract_data_from_bronze(
            table_name=table_name,
            last_processed_key=last_key,
        )

        if pandas_df.empty:
            logger.info(
                "No new data for %s; skipping",
                table_name,
            )
            return {
                "table_name": table_name,
                "status": "no_changes",
                "rows_extracted": 0,
            }

        logger.info(
            "%s: extracted %s rows",
            table_name,
            len(pandas_df),
        )

        # 3. Convert Pandas DataFrame to Spark
        spark = get_spark()
        spark_df = spark.createDataFrame(pandas_df)

        # 4. Apply YAML-driven data quality rules
        validator = DataQualityValidator(table_name=table_name)

        valid_df, quarantine_df = validator.validate(spark_df)

        # Materialize counts before writing or changing state.
        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()

        logger.info(
            "%s: valid=%s, quarantined=%s",
            table_name,
            valid_count,
            quarantine_count,
        )

        # 5. Persist quarantined records to quarantine storage
        if quarantine_count > 0:
            logger.warning(
                "%s: %d quarantined rows detected. Isolating to silver/_quarantine.",
                table_name,
                quarantine_count,
            )
            write_quarantine_to_silver(
                df=quarantine_df.toPandas(),
                table_name=table_name,
            )

        # 6. Write valid records to Silver.
        if valid_count > 0:
            write_dataframe_to_silver(
                df=valid_df.toPandas(),
                table_name=table_name,
            )

        # 7. Commit only after successful processing.
        save_checkpoint(
            checkpoint=CHECKPOINT,
            table_name=table_name,
            last_processed_key=latest_key,
        )

        return {
            "table_name": table_name,
            "status": "success",
            "rows_extracted": len(pandas_df),
            "rows_written": valid_count,
            "rows_quarantined": quarantine_count,
            "last_processed_key": latest_key,
        }

    for table_name in SOURCE_TABLES:
        ingest_table.override(
            task_id=f"ingest_{table_name}"
        )(table_name)


incremental_bronze_to_silver()
