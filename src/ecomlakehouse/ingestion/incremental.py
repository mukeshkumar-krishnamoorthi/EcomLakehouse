import pendulum
from sqlalchemy.engine import Engine

from ecomlakehouse.bronze.writer import write_dataframe_to_bronze
from ecomlakehouse.ingestion.postgres import extract_incremental


SOURCE_TABLES = [
    "categories",
    "products",
    "customers",
    "stores",
    "orders",
    "order_items",
    "payments",
]

PIPELINE_NAME = "postgres_to_bronze"


def run_incremental_ingestion(engine: Engine, **kwargs):

    data_interval_start = kwargs["data_interval_start"]
    data_interval_end = kwargs["data_interval_end"]

    previous_success_end = kwargs["prev_data_interval_end_success"]

    if previous_success_end is not None:
        previous_success_end = pendulum.parse(
            str(previous_success_end)
        )
    else:
        previous_success_end = data_interval_start

    print(f"Current interval start: {data_interval_start}")
    print(f"Current interval end:   {data_interval_end}")
    print(f"Previous successful end: {previous_success_end}")

    for table_name in SOURCE_TABLES:

        print(f"\nProcessing: {table_name}")

        df = extract_incremental(
            engine=engine,
            table_name=table_name,
            start_time=previous_success_end,
            end_time=data_interval_end,
        )

        print(f"Rows extracted: {len(df)}")

        if not df.empty:
            write_dataframe_to_bronze(
                df=df,
                table_name=table_name,
            )
        else:
            print(
                f"No changes found in {table_name} "
                f"between {previous_success_end} "
                f"and {data_interval_end}"
            )
