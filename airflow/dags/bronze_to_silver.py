
from airflow.sdk import dag, task, get_current_context


from ecomlakehouse.utils.logging_utils import get_logger
from ecomlakehouse.silver.extractor import (
    extract_data_from_bronze,
)
from ecomlakehouse.silver.writer import (
    write_dataframe_to_silver,
)

SOURCE_TABLES = [
    "categories",
    "products",
    "customers",
    "stores",
    "orders",
    "order_items",
    "payments",
]

PIPELINE_NAME = "bronze_to_silver"
logger = get_logger(__name__)


@dag(
    dag_id="bronze_to_silver",
    schedule=None,
    tags=["bronze", "silver"],
)
def bronze_to_silver():

    @task
    def ingest_table(
        table_name: str,
        last_processed_key: str | None,
    ) -> dict:
        logger.info(
            "Starting ingestion for table: %s",
            table_name,
        )

        df, latest_key = extract_data_from_bronze(
            table_name=table_name,
            last_processed_key=last_processed_key,
        )

        logger.info(
            "Extracted %s rows from %s",
            len(df),
            table_name,
        )

        if df.empty:
            logger.info(
                "No new data found for %s",
                table_name,
            )

        # TODO:
        # 1. Apply data quality validation.
        # 2. Transform the DataFrame.
        # 3. Write the output to Silver.
        # 4. Only then return the new checkpoint.

        return {
            "table_name": table_name,
            "last_processed_key": latest_key,
            "rows_extracted": len(df),
        }

    @task
    def get_checkpoint(table_name: str) -> str | None:
        context = get_current_context()
        ti = context["ti"]

        return ti.xcom_pull(
            task_ids=f"save_checkpoint_{table_name}",
            key="last_processed_key",
        )

    @task
    def save_checkpoint(result: dict) -> None:

        context = get_current_context()
        ti = context["ti"]

        ti.xcom_push(
            key="last_processed_key",
            value=result["last_processed_key"],
        )

    for table_name in SOURCE_TABLES:
        checkpoint = get_checkpoint.override(
            task_id=f"get_checkpoint_{table_name}"
        )(table_name)

        result = ingest_table.override(
            task_id=f"ingest_{table_name}"
        )(table_name, checkpoint)

        save_checkpoint.override(
            task_id=f"save_checkpoint_{table_name}"
        )(result)


bronze_to_silver()
