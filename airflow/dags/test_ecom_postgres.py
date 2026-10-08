from datetime import datetime

from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook


@dag(
    dag_id="test_ecom_postgres",
    start_date=datetime(2026, 10, 7),
    schedule=None,
    catchup=False,
    tags=["test", "connection"],
)
def test_ecom_postgres():
    """
    DAG to test the connection to the ecom_postgres database.
    """

    @task
    def test_connection():
        hook = BaseHook.get_hook(conn_id="ecom_postgres")

        result = hook.get_first(
            "SELECT COUNT(*) FROM orders"
        )

        print(f"Orders in source database: {result[0]}")

    test_connection()


test_ecom_postgres()
