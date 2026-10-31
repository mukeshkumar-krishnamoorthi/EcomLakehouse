from datetime import UTC

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


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
    start_time: str | None,
    end_time: str | None,
) -> pd.DataFrame:

    if end_time is None or str(end_time).strip().lower() in ("none", "", "null"):
        from datetime import datetime
        end_time = datetime.now(UTC)

    is_start_empty = (
        start_time is None
        or str(start_time).strip().lower() in ("none", "", "null")
    )

    if is_start_empty:
        # Initial run: no previous checkpoint exists, extract all historical records up to end_time
        query = text(f"""
            SELECT *
            FROM {table_name}
            WHERE updated_at <= :end_time
        """)
        params = {"end_time": str(end_time)}
    else:
        query = text(f"""
            SELECT *
            FROM {table_name}
            WHERE updated_at > :start_time
              AND updated_at <= :end_time
        """)
        params = {
            "start_time": str(start_time),
            "end_time": str(end_time),
        }

    return pd.read_sql(
        query,
        engine,
        params=params,
    )
