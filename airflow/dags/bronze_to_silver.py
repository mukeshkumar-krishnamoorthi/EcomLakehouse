import os
import shutil
from datetime import UTC, datetime

from airflow.sdk import Asset, dag, get_current_context, task

from ecomlakehouse.constents.tables import BRONZE_TABLES
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
SILVER_INGESTION_ASSET = Asset(
    "ecomlakehouse://silver/ingestion_completed"
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
    max_active_runs=1,
    max_active_tasks=2,
    doc_md='''
    ### Bronze to Silver Ingestion DAG
    This DAG incrementally transforms Bronze-layered data into Silver-layered tables.
    It uses a checkpointing mechanism to track the last processed key for each table.
    '''
)
def incremental_bronze_to_silver():

    @task
    def read_checkpoints() -> dict:
        """Read persistent watermarks for all bronze tables."""

        checkpoints = {
            table_name: get_checkpoint(
                checkpoint=CHECKPOINT,
                table_name=table_name,
            )
            for table_name in BRONZE_TABLES
        }

        logger.info("Loaded %d checkpoints", len(checkpoints))
        return checkpoints

    @task
    def transform_data(
        table_name: str,
        checkpoint: str | None
    ) -> dict:
        """Extract bronze data and stage it in MinIO as Parquet."""

        context = get_current_context()
        run_id = context.get("run_id")

        last_key = (
            (checkpoint.get("watermark_end")
             or checkpoint.get("last_processed_key"))
            if isinstance(checkpoint, dict)
            else checkpoint
        )

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

        rows_extracted = spark_df.count()
        logger.info(
            "Extracted %d rows from %s (key range: %s…%s)",
            rows_extracted,
            table_name,
            last_key or "(start)",
            latest_key,
        )

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
                "watermark_start": str(last_key) if last_key else None,
                "watermark_end": latest_key,
                "committed_files": committed_files,
            }
        finally:
            try:
                spark.stop()
            except Exception:
                pass
            if cache_dir and os.path.exists(cache_dir):
                shutil.rmtree(cache_dir, ignore_errors=True)

    @task
    def commit_checkpoint(result: dict) -> dict:
        """Advance the checkpoint only after a successful silver write."""

        if result["status"] == "no_changes":
            return result

        context = get_current_context()
        run_id = context["run_id"]
        checkpoint_val = result.get("watermark_end") or result.get("last_processed_key")

        save_checkpoint(
            checkpoint=CHECKPOINT,
            table_name=result["table_name"],
            last_processed_key=checkpoint_val,
            run_id=run_id,
            watermark_start=result.get("watermark_start"),
            watermark_end=checkpoint_val,
            rows_extracted=result.get("rows_extracted", 0),
            rows_written=result.get("rows_written", 0),
            rows_quarantined=result.get("rows_quarantined", 0),
            committed_files=result.get("committed_files", []),
            status=result.get("status", "success"),
        )

        logger.info(
            "Checkpoint committed | table=%s | checkpoint=%s",
            result["table_name"],
            checkpoint_val,
        )

        return {
            "table_name": result["table_name"],
            "status": "success",
            "rows": result.get("rows_written", 0),
            "checkpoint": checkpoint_val,
        }

    @task(outlets=[SILVER_INGESTION_ASSET])
    def complete_silver_ingestion(commits: list) -> dict:
        """Signal that all source tables have finished bronze ingestion."""
        logger.info(
            "Bronze ingestion completed for all %d tables. Emitted Asset event: %s",
            len(commits),
            SILVER_INGESTION_ASSET.uri,
        )
        return {"status": "completed", "tables": len(commits)}

    checkpoints = read_checkpoints()
    commits = []

    for table_name in BRONZE_TABLES:

        result = transform_data.override(
            task_id=f"transform_{table_name}"
        )(
            table_name=table_name,
            checkpoint=checkpoints[table_name]
        )

        commit = commit_checkpoint.override(
            task_id=f"commit_{table_name}"
        )(result)

        commits.append(commit)

    complete_silver_ingestion(commits)


incremental_bronze_to_silver()
