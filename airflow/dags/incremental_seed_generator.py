from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="incremental_seed_generator",
    start_date=datetime(2026, 10, 7),
    schedule="*/10 * * * *",
    catchup=False,
    tags=["Incremental", "Seed"],
) as dag:

    run_incremental_seed_generation = BashOperator(
        task_id="run_incremental_seed_generation",
        bash_command=(
            "/home/airflow/.local/bin/python "
            "/opt/ecomlakehouse/src/ecomlakehouse/generator/incremental_seed_generator.py"
        ),
    )
