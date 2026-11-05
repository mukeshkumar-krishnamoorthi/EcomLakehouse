import os
import shutil
from datetime import UTC, datetime

from airflow.sdk import Asset, dag, get_current_context, task

from ecomlakehouse.constents.tables import SOURCE_TABLES
from ecomlakehouse.ingestion.checkpoint import (
    get_checkpoint,
    save_checkpoint,
)
from ecomlakehouse.quality.validator import DataQualityValidator
from ecomlakehouse.silver.extractor import extract_data_from_bronze
from ecomlakehouse.silver.writer import (
    write_df_to_silver,
    write_quarantine_to_silver,
)
from ecomlakehouse.utils.logging_utils import get_logger
from ecomlakehouse.utils.spark import get_spark

logger = get_logger(__name__)

CHECKPOINT = "BRONZE_TO_SILVER"

BRONZE_INGESTION_ASSET = Asset(
    "ecomlakehouse://bronze/ingestion_completed"
)

TABLE_PRIMARY_KEYS = {
    "categories": ["category_id"],
    "products": ["product_id"],
    "customers": ["customer_id"],
    "stores": ["store_id"],
    "payment_methods": ["payment_method_id"],
    "orders": ["order_id"],
    "order_items": ["order_item_id"],
    "payments": ["payment_id"],
}


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
        context = get_current_context()
        run_id = context.get("run_id")

        # 1. Load active checkpoint
        checkpoint_data = get_checkpoint(
            checkpoint=CHECKPOINT,
            table_name=table_name,
        )
        last_key = (
            (checkpoint_data.get("watermark_end")
             or checkpoint_data.get("last_processed_key"))
            if isinstance(checkpoint_data, dict)
            else checkpoint_data
        )

        # 2. Extract bounded batch of new Bronze files directly into Spark
        spark = get_spark()
        spark_df, latest_key, has_more, cache_dir = extract_data_from_bronze(
            spark=spark,
            table_name=table_name,
            last_processed_key=last_key,
        )

        if spark_df is None:
            logger.info(
                "No new data for %s; skipping",
                table_name,
            )
            return {
                "table_name": table_name,
                "status": "no_changes",
                "rows_extracted": 0,
            }

        try:
            # 3. Apply YAML-driven data quality rules
            validator = DataQualityValidator(table_name=table_name)
            valid_df, quarantine_df = validator.validate(spark_df)

            # Deduplicate valid records within batch on primary key
            pk_cols = TABLE_PRIMARY_KEYS.get(table_name, [])
            if pk_cols:
                valid_df = valid_df.dropDuplicates(pk_cols)

            # Persist across actions to prevent recomputation
            valid_df.persist()
            quarantine_df.persist()

            valid_count = valid_df.count()
            quarantine_count = quarantine_df.count()
            total_extracted = valid_count + quarantine_count

            logger.info(
                "%s: extracted=%s, valid=%s, quarantined=%s (has_more_files=%s)",
                table_name,
                total_extracted,
                valid_count,
                quarantine_count,
                has_more,
            )

            committed_files = []

            # 4. Persist quarantined records to quarantine storage using Spark
            if quarantine_count > 0:
                logger.warning(
                    "%s: %d quarantined rows detected. Isolating to silver/_quarantine.",
                    table_name,
                    quarantine_count,
                )
                q_files = write_quarantine_to_silver(
                    df=quarantine_df,
                    table_name=table_name,
                    run_id=run_id,
                )
                committed_files.extend(q_files)

            # 5. Write valid records to Silver using Spark
            if valid_count > 0:
                s_files = write_df_to_silver(
                    df=valid_df,
                    table_name=table_name,
                    run_id=run_id,
                )
                committed_files.extend(s_files)

            # 6. Commit checkpoint (both active pointer and historical run archive)
            save_checkpoint(
                checkpoint=CHECKPOINT,
                table_name=table_name,
                last_processed_key=latest_key,
                run_id=run_id,
                watermark_start=str(last_key) if last_key else None,
                watermark_end=latest_key,
                rows_extracted=total_extracted,
                committed_files=committed_files,
                status="success",
            )

            valid_df.unpersist()
            quarantine_df.unpersist()

            if has_more:
                logger.info(
                    "%s has more pending Bronze files; will continue in subsequent run.",
                    table_name,
                )

            return {
                "table_name": table_name,
                "status": "success",
                "rows_extracted": total_extracted,
                "rows_written": valid_count,
                "rows_quarantined": quarantine_count,
                "last_processed_key": latest_key,
                "has_more": has_more,
            }
        finally:
            if cache_dir and os.path.exists(cache_dir):
                shutil.rmtree(cache_dir, ignore_errors=True)

    for table_name in SOURCE_TABLES:
        ingest_table.override(
            task_id=f"ingest_{table_name}"
        )(table_name)


incremental_bronze_to_silver()
