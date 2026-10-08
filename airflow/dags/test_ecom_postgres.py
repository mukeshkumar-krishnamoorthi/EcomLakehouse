"""
EcomLakehouse connection verification DAG.

Tests connectivity to:

1. E-Commerce PostgreSQL database
2. Airflow metadata database
3. MinIO / S3 object storage
4. Environment configuration
5. Other configured Airflow connections
"""

from __future__ import annotations
import logging

import os
from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    HAS_S3_HOOK = True
except ImportError:
    S3Hook = None
    HAS_S3_HOOK = False

from ecomlakehouse.utils.logging_utils import get_logger

try:
    from ecomlakehouse.utils.storage import (
        get_bucket_name,
        get_s3_client,
    )

    HAS_ECOM_STORAGE = True
except ImportError:
    get_bucket_name = None
    get_s3_client = None
    HAS_ECOM_STORAGE = False


logger = get_logger(__name__)


@dag(
    dag_id="test_ecom_postgres",
    start_date=datetime(2026, 10, 7),
    schedule=None,
    catchup=False,
    tags=[
        "test",
        "connection",
        "postgres",
        "minio",
        "airflow_db",
    ],
    doc_md="""
    ### Comprehensive Connection Verification DAG

    Tests connectivity to:

    1. **E-Commerce PostgreSQL Source DB**
       (`ecom_postgres` connection via `PostgresHook`)

    2. **Airflow Metadata Database**
       (`airflow_db` connection and Airflow settings engine)

    3. **MinIO S3 Object Storage**
       (`get_s3_client()` / `S3Hook`)

    4. **Environment & System Configuration**
       (`POSTGRES_*`, `S3_*`)

    5. **Other Airflow Connections**
       (`aws_default`, `postgres_default`, etc.)
    """,
)
def test_ecom_postgres():

    @task
    def test_environment_configs() -> dict:
        """
        Validate environment variables for database
        and object storage configuration.
        """

        logger.info("=== Checking Environment Variables ===")

        env_vars = {
            "POSTGRES_HOST": os.getenv(
                "POSTGRES_HOST",
                "not set",
            ),
            "POSTGRES_PORT": os.getenv(
                "POSTGRES_PORT",
                "not set",
            ),
            "POSTGRES_USER": os.getenv(
                "POSTGRES_USER",
                "not set",
            ),
            "POSTGRES_DB": os.getenv(
                "POSTGRES_DB",
                "not set",
            ),
            "S3_ENDPOINT": os.getenv(
                "S3_ENDPOINT",
                "not set",
            ),
            "S3_BUCKET": os.getenv(
                "S3_BUCKET",
                "not set",
            ),
            "S3_REGION": os.getenv(
                "S3_REGION",
                "not set",
            ),
        }

        for key, value in env_vars.items():
            logger.info(
                "Environment variable: %s=%s",
                key,
                value,
            )

        return {
            "status": "SUCCESS",
            "configs": env_vars,
        }

    @task
    def test_ecom_postgres_connection() -> dict:
        """
        Test the ecom_postgres connection and verify
        database queries.
        """

        logger.info(
            "=== Testing E-Commerce PostgreSQL Connection "
            "(ecom_postgres) ==="
        )

        hook = PostgresHook(
            postgres_conn_id="ecom_postgres",
        )

        # --------------------------------------------------
        # PostgreSQL version
        # --------------------------------------------------

        version_result = hook.get_first(
            "SELECT version();"
        )

        if not version_result:
            raise RuntimeError(
                "PostgreSQL version query returned no result."
            )

        version = version_result[0]

        logger.info(
            "Postgres Version: %s",
            version,
        )

        # --------------------------------------------------
        # Public table count
        # --------------------------------------------------

        table_result = hook.get_first(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'public';
            """
        )

        tables_count = table_result[0]

        logger.info(
            "Public Schema Table Count: %s",
            tables_count,
        )

        # --------------------------------------------------
        # Orders count
        # --------------------------------------------------

        orders_result = hook.get_first(
            "SELECT COUNT(*) FROM orders;"
        )

        orders_count = orders_result[0]

        logger.info(
            "Total Orders in ecom database: %s",
            orders_count,
        )

        # --------------------------------------------------
        # SQLAlchemy connection
        # --------------------------------------------------

        logger.info(
            "Testing SQLAlchemy engine connection."
        )

        engine = hook.get_sqlalchemy_engine()

        with engine.connect() as connection:
            connection_result = (
                connection.exec_driver_sql(
                    "SELECT 1"
                ).scalar()
            )

        logger.info(
            "SQLAlchemy SELECT 1 result: %s",
            connection_result,
        )

        logger.info(
            "E-Commerce PostgreSQL connection test "
            "completed successfully."
        )

        return {
            "status": "SUCCESS",
            "postgres_version": version,
            "tables_count": tables_count,
            "orders_count": orders_count,
        }

    @task
    def test_airflow_db_connection() -> dict:
        """
        Test the Airflow metadata database connection.
        """

        logger.info(
            "=== Testing Airflow Metastore DB Connection ==="
        )

        success = False
        details: dict = {}

        # --------------------------------------------------
        # Strategy A:
        # Try known PostgreSQL connections
        # --------------------------------------------------

        for conn_id in [
            "airflow_db",
            "postgres_default",
        ]:

            try:
                logger.info(
                    "Testing Airflow DB connection: %s",
                    conn_id,
                )

                hook = PostgresHook(
                    postgres_conn_id=conn_id,
                )

                result = hook.get_first(
                    "SELECT 1;"
                )

                if result:

                    logger.info(
                        "Connected to Airflow DB using "
                        "conn_id=%s",
                        conn_id,
                    )

                    dag_result = hook.get_first(
                        "SELECT COUNT(*) FROM dag;"
                    )

                    dag_count = dag_result[0]

                    logger.info(
                        "Registered DAG count: %s",
                        dag_count,
                    )

                    success = True

                    details = {
                        "conn_id": conn_id,
                        "dag_count": dag_count,
                    }

                    break

            except Exception:
                logger.exception(
                    "Unable to connect using conn_id=%s",
                    conn_id,
                )

        # --------------------------------------------------
        # Strategy B:
        # Airflow internal settings engine
        # --------------------------------------------------

        if not success:

            try:
                from airflow.settings import engine

                logger.info(
                    "Trying Airflow settings.engine."
                )

                with engine.connect() as connection:

                    result = (
                        connection
                        .exec_driver_sql(
                            "SELECT 1"
                        )
                        .scalar()
                    )

                logger.info(
                    "Airflow settings.engine "
                    "connection successful: %s",
                    result,
                )

                success = True

                details = {
                    "conn_type": (
                        "airflow.settings.engine"
                    ),
                }

            except Exception:
                logger.exception(
                    "Airflow settings.engine connection failed."
                )

        if not success:
            raise RuntimeError(
                "Failed to establish connection "
                "to Airflow Metadata DB."
            )

        return {
            "status": "SUCCESS",
            **details,
        }

    @task
    def test_minio_connection() -> dict:
        """
        Test MinIO / S3 object storage.
        """

        logger.info(
            "=== Testing MinIO / S3 Object Storage Connection ==="
        )

        results: dict = {}

        # --------------------------------------------------
        # Method 1:
        # EcomLakehouse storage utility
        # --------------------------------------------------

        if HAS_ECOM_STORAGE:

            try:
                logger.info(
                    "Testing MinIO using EcomLakehouse "
                    "storage utility."
                )

                s3_client = get_s3_client()
                bucket_name = get_bucket_name()

                response = s3_client.list_buckets()

                buckets = [
                    bucket["Name"]
                    for bucket in response.get(
                        "Buckets",
                        [],
                    )
                ]

                logger.info(
                    "MinIO buckets found: count=%d buckets=%s",
                    len(buckets),
                    buckets,
                )

                # Check target bucket

                s3_client.head_bucket(
                    Bucket=bucket_name,
                )

                logger.info(
                    "Target bucket is accessible: %s",
                    bucket_name,
                )

                # Sample objects

                objects_response = (
                    s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        MaxKeys=5,
                    )
                )

                object_count = objects_response.get(
                    "KeyCount",
                    0,
                )

                logger.info(
                    "Objects found in bucket=%s count=%d",
                    bucket_name,
                    object_count,
                )

                results["storage_util"] = "SUCCESS"
                results["buckets"] = buckets
                results["target_bucket"] = bucket_name

            except Exception:

                logger.exception(
                    "MinIO connection test using "
                    "get_s3_client() failed."
                )

                results["storage_util"] = "FAILED"

        else:

            logger.info(
                "EcomLakehouse storage utility is not available."
            )

        # --------------------------------------------------
        # Method 2:
        # Airflow S3Hook
        # --------------------------------------------------

        if HAS_S3_HOOK:

            for aws_conn_id in [
                "aws_default",
                "minio_s3",
            ]:

                try:

                    logger.info(
                        "Testing Airflow S3Hook connection: %s",
                        aws_conn_id,
                    )

                    s3_hook = S3Hook(
                        aws_conn_id=aws_conn_id,
                    )

                    buckets = s3_hook.list_buckets()

                    logger.info(
                        "S3Hook connection successful: "
                        "conn_id=%s buckets=%s",
                        aws_conn_id,
                        buckets,
                    )

                    results[
                        f"s3_hook_{aws_conn_id}"
                    ] = "SUCCESS"

                    break

                except Exception:

                    logger.exception(
                        "S3Hook connection failed: %s",
                        aws_conn_id,
                    )

        # --------------------------------------------------
        # Determine final status
        # --------------------------------------------------

        s3_verified = (
            results.get("storage_util") == "SUCCESS"
            or any(
                value == "SUCCESS"
                for key, value in results.items()
                if key.startswith("s3_hook")
            )
        )

        if s3_verified:

            logger.info(
                "MinIO / S3 storage verification successful."
            )

        else:

            logger.warning(
                "MinIO / S3 connection could not be "
                "fully verified."
            )

        return {
            "status": "SUCCESS" if s3_verified else "WARNING",
            "details": results,
        }

    @task
    def test_other_connections() -> dict:
        """
        Test additional Airflow connections.
        """

        logger.info(
            "=== Testing Other Airflow Connections ==="
        )

        connection_statuses = {}

        test_connections = [
            "postgres_default",
            "aws_default",
            "trino_default",
            "spark_default",
        ]

        for conn_id in test_connections:

            try:

                connection = BaseHook.get_connection(
                    conn_id
                )

                connection_statuses[conn_id] = (
                    f"Configured ({connection.conn_type})"
                )

                logger.info(
                    "Airflow connection configured: "
                    "conn_id=%s type=%s",
                    conn_id,
                    connection.conn_type,
                )

            except Exception:

                connection_statuses[conn_id] = (
                    "Not Configured"
                )

                logger.info(
                    "Airflow connection not configured: %s",
                    conn_id,
                )

        return {
            "status": "COMPLETED",
            "connection_summary": connection_statuses,
        }

    @task
    def summary_report(
        env_res: dict,
        postgres_res: dict,
        airflow_db_res: dict,
        minio_res: dict,
        other_res: dict,
    ) -> None:
        """
        Log a complete health report.
        """

        logger.info(
            "=================================================="
        )

        logger.info(
            "       ECOM LAKEHOUSE CONNECTION TEST SUMMARY"
        )

        logger.info(
            "=================================================="
        )

        # Environment

        logger.info(
            "1. Environment Configs: %s",
            env_res.get("status"),
        )

        # PostgreSQL

        logger.info(
            "2. Postgres (ecom_db): %s | "
            "Orders=%s | Tables=%s",
            postgres_res.get("status"),
            postgres_res.get("orders_count"),
            postgres_res.get("tables_count"),
        )

        # Airflow DB

        airflow_detail = airflow_db_res.get(
            "conn_id",
            airflow_db_res.get("conn_type"),
        )

        logger.info(
            "3. Airflow Metastore DB: %s | Detail=%s",
            airflow_db_res.get("status"),
            airflow_detail,
        )

        # MinIO

        minio_details = minio_res.get(
            "details",
            {},
        )

        logger.info(
            "4. MinIO S3 Storage: %s | Buckets=%s",
            minio_res.get("status"),
            minio_details.get(
                "buckets",
                [],
            ),
        )

        # Other connections

        configured_connections = [
            key
            for key, value in other_res.get(
                "connection_summary",
                {},
            ).items()
            if "Configured" in value
        ]

        logger.info(
            "5. Other Connections: %s | Configured=%s",
            other_res.get("status"),
            configured_connections,
        )

        logger.info(
            "=================================================="
        )

    # ------------------------------------------------------
    # DAG dependency graph
    # ------------------------------------------------------

    env_res = test_environment_configs()

    postgres_res = test_ecom_postgres_connection()

    airflow_db_res = test_airflow_db_connection()

    minio_res = test_minio_connection()

    other_res = test_other_connections()

    summary_report(
        env_res,
        postgres_res,
        airflow_db_res,
        minio_res,
        other_res,
    )


test_ecom_postgres()
