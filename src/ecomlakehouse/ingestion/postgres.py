from sqlalchemy import text
from sqlalchemy.engine import Engine

import pandas as pd


def extract_table(
    engine: Engine,
    table_name: str,
) -> pd.DataFrame:

    query = f"SELECT * FROM {table_name}"

    return pd.read_sql(
        query,
        engine,
    )


def extract_incremental(
    engine: Engine,
    table_name: str,
    start_time,
    end_time,
) -> pd.DataFrame:

    query = text(f"""
        SELECT *
        FROM {table_name}
        WHERE updated_at >= :start_time
          AND updated_at < :end_time
    """)

    return pd.read_sql(
        query,
        engine,
        params={
            "start_time": start_time,
            "end_time": end_time,
        },
    )
