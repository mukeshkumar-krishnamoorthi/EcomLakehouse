
import io
import uuid

import pandas as pd
import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

from ecomlakehouse.bronze.writer import write_dataframe_to_bronze
from ecomlakehouse.constents.tables import SOURCE_TABLES
from ecomlakehouse.ingestion.checkpoint import (
    get_checkpoint,
    save_checkpoint,
)
from ecomlakehouse.ingestion.postgres import extract_incremental
from ecomlakehouse.utils.logging_utils import get_logger
from ecomlakehouse.utils.postgres import get_connection
from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)

logger = get_logger(__name__)


CHECKPOINT = "BRONZE_INGESTION"

BRONZE_INGESTION_ASSET = Asset("ecomlakehouse://bronze/ingestion_completed")
TEMP_PREFIX = "bronze/_staging"


def temp_key(run_id: str, table_name: str) -> str:
    safe_run_id = uuid.uuid5(
        uuid.NAMESPACE_URL,
        run_id,
    ).hex

    return f"{TEMP_PREFIX}/{safe_run_id}/{table_name}.parquet"


@dag(
    dag_id="incremental_bronze_ingestion",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="0/5 * * * *",
    catchup=False,
    tags=["Incremental", "Postgres", "Bronze"],
)
def incremental_bronze_ingestion():

    @task
    def read_checkpoints() -> dict:
        """Read persistent watermarks for all source tables."""

        checkpoints = {
            table_name: get_checkpoint(
                checkpoint=CHECKPOINT,
                table_name=table_name,
            )
            for table_name in SOURCE_TABLES
        }

        logger.info("Loaded %d checkpoints", len(checkpoints))
        return checkpoints

    @task
    def extract_table(
        table_name: str,
        checkpoint: str | None,
    ) -> dict:
        """Extract source rows and stage them in MinIO as Parquet."""

        start_time = (
            (checkpoint.get("watermark_end") or checkpoint.get("last_processed_key"))
            if isinstance(checkpoint, dict)
            else checkpoint
        )

        context = get_current_context()
        run_id = context["run_id"]
        raw_end = context.get("data_interval_end") or pendulum.now("UTC")
        end_time = raw_end.subtract(minutes=2)

        engine = get_connection(conn_id="ecom_postgres")

        try:
            logger.info(
                "Extracting table=%s from start_time=%s until=%s",
                table_name,
                start_time,
                end_time,
            )

            df = extract_incremental(
                engine=engine,
                table_name=table_name,
                start_time=start_time,
                end_time=end_time,
            )

            if df.empty:
                return {
                    "table_name": table_name,
                    "status": "no_changes",
                    "stage_key": None,
                    "latest_checkpoint": start_time,
                    "start_time": start_time,
                    "rows": 0,
                }

            timestamps = pd.to_datetime(
                df["updated_at"],
                utc=True,
                errors="raise",
            )

            if timestamps.isna().any():
                raise ValueError(
                    f"{table_name} contains null updated_at values."
                )

            latest_checkpoint = timestamps.max().isoformat()

            if (
                start_time is not None
                and pd.Timestamp(latest_checkpoint)
                <= pd.Timestamp(start_time)
            ):
                return {
                    "table_name": table_name,
                    "status": "no_changes",
                    "stage_key": None,
                    "latest_checkpoint": start_time,
                    "start_time": start_time,
                    "rows": 0,
                }

            if "updated_at" not in df.columns:
                raise ValueError(
                    f"{table_name} has no updated_at column. "
                    "Configure a table-specific watermark."
                )

            buffer = io.BytesIO()
            df.to_parquet(
                buffer,
                index=False,
                engine="pyarrow",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

            key = temp_key(run_id, table_name)

            get_s3_client().put_object(
                Bucket=get_bucket_name(),
                Key=key,
                Body=buffer.getvalue(),
                ContentType="application/vnd.apache.parquet",
            )

            return {
                "table_name": table_name,
                "status": "extracted",
                "stage_key": key,
                "latest_checkpoint": latest_checkpoint,
                "start_time": start_time,
                "rows": len(df),
            }

        finally:
            engine.dispose()

    @task
    def transform_table(extraction: dict) -> dict:
        """Read staged data and apply optional Bronze preparation."""

        if extraction["status"] == "no_changes":
            return extraction

        s3 = get_s3_client()
        bucket = get_bucket_name()

        response = s3.get_object(
            Bucket=bucket,
            Key=extraction["stage_key"],
        )

        df = pd.read_parquet(
            io.BytesIO(response["Body"].read())
        )

        context = get_current_context()
        transformed_df = df.copy()
        transformed_df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
        transformed_df["_source_table"] = extraction["table_name"]
        transformed_df["_run_id"] = str(context.get("run_id"))

        buffer = io.BytesIO()
        transformed_df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )

        transformed_key = (
            extraction["stage_key"].removesuffix(".parquet")
            + "_transformed.parquet"
        )

        s3.put_object(
            Bucket=bucket,
            Key=transformed_key,
            Body=buffer.getvalue(),
            ContentType="application/vnd.apache.parquet",
        )

        return {
            **extraction,
            "stage_key": transformed_key,
            "rows": len(transformed_df),
        }

    @task
    def write_bronze(result: dict) -> dict:
        """Write each staged increment to Bronze."""

        if result["status"] == "no_changes":
            return result

        s3 = get_s3_client()
        bucket = get_bucket_name()

        response = s3.get_object(
            Bucket=bucket,
            Key=result["stage_key"],
        )

        df = pd.read_parquet(
            io.BytesIO(response["Body"].read())
        )

        object_key = write_dataframe_to_bronze(
            df=df,
            table_name=result["table_name"],
        )

        logger.info(
            "Bronze write completed | table=%s | rows=%d | object=%s",
            result["table_name"],
            len(df),
            object_key,
        )

        return {
            **result,
            "status": "success",
            "bronze_object_key": object_key,
            "rows": len(df),
        }

    @task
    def commit_checkpoint(result: dict) -> dict:
        """Advance the checkpoint only after a successful Bronze write."""

        if result["status"] == "no_changes":
            return result

        context = get_current_context()
        run_id = context["run_id"]

        save_checkpoint(
            checkpoint=CHECKPOINT,
            table_name=result["table_name"],
            last_processed_key=result["latest_checkpoint"],
            run_id=run_id,
            watermark_start=result.get("start_time"),
            watermark_end=result["latest_checkpoint"],
            rows_extracted=result.get("rows", 0),
            committed_files=[result["bronze_object_key"]],
            status=result["status"]
        )

        logger.info(
            "Checkpoint committed | table=%s | checkpoint=%s",
            result["table_name"],
            result["latest_checkpoint"],
        )

        return {
            "table_name": result["table_name"],
            "status": "success",
            "rows": result["rows"],
            "checkpoint": result["latest_checkpoint"],
        }

    @task(outlets=[BRONZE_INGESTION_ASSET])
    def complete_ingestion(commits: list) -> dict:
        """Signal that all source tables have finished bronze ingestion."""
        logger.info(
            "Bronze ingestion completed for all %d tables. Emitted Asset event: %s",
            len(commits),
            BRONZE_INGESTION_ASSET.uri,
        )
        return {"status": "completed", "tables": len(commits)}

    checkpoints = read_checkpoints()
    commits = []

    for table_name in SOURCE_TABLES:
        extracted = extract_table.override(
            task_id=f"extract_{table_name}",
        )(
            table_name=table_name,
            checkpoint=checkpoints[table_name],
        )

        transformed = transform_table.override(
            task_id=f"transform_{table_name}",
        )(extracted)

        written = write_bronze.override(
            task_id=f"write_bronze_{table_name}",
        )(transformed)

        committed = commit_checkpoint.override(
            task_id=f"checkpoint_{table_name}",
        )(written)

        commits.append(committed)

    complete_ingestion(commits)


incremental_bronze_ingestion()
