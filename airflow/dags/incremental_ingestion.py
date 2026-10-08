from pendulum import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ecomlakehouse.ingestion.incremental import run_incremental_ingestion


@dag(
    dag_id="incremental_ingestion",
    start_date=datetime(2026, 10, 1, tz="UTC"),
    schedule="0/30 * * * *",
    catchup=False,
    tags=["Incremental", "Postgres"],
)
def incremental_ingestion():

    @task
    def ingest(**kwargs):

        hook = PostgresHook(
            postgres_conn_id="ecom_postgres"
        )

        engine = hook.get_sqlalchemy_engine()

        run_incremental_ingestion(
            engine,
            **kwargs
        )

    ingest()


incremental_ingestion()
