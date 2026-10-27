import os

from ecomlakehouse.utils.spark import get_spark
from pyspark.sql import DataFrame, SparkSession


def test_spark(
    spark: SparkSession,
    jdbc_url: str,
    table: str,
    user: str,
    password: str,
    driver: str = "org.postgresql.Driver",
) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .load()
    )


if __name__ == "__main__":
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5441")
    db_name = os.getenv("POSTGRES_DB", "ecommerce")
    db_user = os.getenv("POSTGRES_USER", "ecom_user")
    db_password = os.getenv("POSTGRES_PASSWORD", "ecom_password")
    table = os.getenv("POSTGRES_TABLE", "public.orders")

    spark = get_spark()

    try:
        result = test_spark(
            spark=spark,
            jdbc_url=f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
            table=table,
            user=db_user,
            password=db_password,
        )

        result.show(10, truncate=False)
        result.printSchema()
    finally:
        spark.stop()
