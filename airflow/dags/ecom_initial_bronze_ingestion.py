from airflow.sdk import dag, task

from airflow.providers.postgres.hooks.postgres import PostgresHook

from ecomlakehouse.ingestion.batch import run_batch_ingestion


@dag(
    dag_id="ecom_initial_bronze_ingestion",
    start_date=None,
    schedule=None,
    catchup=False,
    tags=["Batch"],
)
def ecom_initial_bronze_ingestion():
    """
    Run initial batch ingestion for all source tables.
    """

    @task
    def ingest() -> None:
        """
        Ingest all source tables to bronze layer.
        """
        hook = PostgresHook(
            postgres_conn_id="ecom_postgres"
        )

        engine = hook.get_sqlalchemy_engine()

        run_batch_ingestion(engine)

    ingest()


ecom_initial_bronze_ingestion()
