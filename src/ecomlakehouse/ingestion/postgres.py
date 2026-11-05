import os
from datetime import UTC

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

DEFAULT_MAX_ROWS_PER_EXTRACTION = int(
    os.getenv("DEFAULT_MAX_ROWS_PER_EXTRACTION", "250000")
)


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
    max_rows: int = DEFAULT_MAX_ROWS_PER_EXTRACTION,
) -> pd.DataFrame:

    if end_time is None or str(end_time).strip().lower() in ("none", "", "null"):
        from datetime import datetime
        end_time = datetime.now(UTC)

    is_start_empty = (
        start_time is None
        or str(start_time).strip().lower() in ("none", "", "null")
    )

    if is_start_empty:
        # Initial run: extract historical records up to end_time, bounded by max_rows
        query = text(f"""
            SELECT *
            FROM {table_name}
            WHERE updated_at <= :end_time
            ORDER BY updated_at ASC
            LIMIT :limit
        """)
        params = {"end_time": str(end_time), "limit": max_rows}
    else:
        query = text(f"""
            SELECT *
            FROM {table_name}
            WHERE updated_at > :start_time
              AND updated_at <= :end_time
            ORDER BY updated_at ASC
            LIMIT :limit
        """)
        params = {
            "start_time": str(start_time),
            "end_time": str(end_time),
            "limit": max_rows,
        }

    df = pd.read_sql(
        query,
        engine,
        params=params,
    )

    # Safe boundary trimming: if limit reached, exclude the trailing max timestamp
    # so the subsequent incremental run extracts all rows sharing that timestamp.
    if len(df) >= max_rows and not df.empty and "updated_at" in df.columns:
        max_ts = df["updated_at"].max()
        if df["updated_at"].min() != max_ts:
            df = df[df["updated_at"] < max_ts]

    return df
