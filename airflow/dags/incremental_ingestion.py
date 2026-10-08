from ecomlakehouse.utils.postgres import get_connection
from pendulum import datetime

from airflow.sdk import dag, task

from ecomlakehouse.ingestion.incremental import run_incremental_ingestion
from ecomlakehouse.utils.postgres import get_connection
from ecomlakehouse.utils.logging_utils import get_logger


logger = get_logger(__name__)


@dag(
    dag_id="incremental_ingestion",
    start_date=datetime(2026, 10, 1, tz="UTC"),
    schedule="0/30 * * * *",
    catchup=False,
    tags=["Incremental", "Postgres"],
)
def incremental_ingestion():

    @task
    def incremental_load(**kwargs):

        logger.info("\nStarting incremental ingestion")

        engine = get_connection(conn_id="ecom_postgres")

        logger.info("Created SQLAlchemy engine")

        run_incremental_ingestion(
            engine,
            **kwargs,
        )

        logger.info("\nIncremental ingestion completed successfully")

    incremental_load()


incremental_ingestion()
