from sqlalchemy.engine import Engine

from airflow.providers.postgres.hooks.postgres import PostgresHook
from ecomlakehouse.utils.logging_utils import get_logger

logger = get_logger(__name__)


def get_connection(conn_id: str = "ecom_postgres") -> Engine:
    """
    Get SQLAlchemy engine for the given connection ID.

    Args:
        conn_id (str): Airflow connection ID for the PostgreSQL database.

    Returns:
        Engine: SQLAlchemy engine for the given connection ID.
    """

    logger.info("\nGetting connection for %s", conn_id)

    hook = PostgresHook(
        postgres_conn_id=conn_id
    )

    logger.info("Created PostgresHook")

    engine = hook.get_sqlalchemy_engine()

    logger.info("Connection established")

    return engine
