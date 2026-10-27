import os

from pyspark.sql import SparkSession


def get_spark(include_packages: bool = False):
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://ecom-minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin123")

    builder = (
        SparkSession.builder
        .appName("EcomLakehouse")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    if include_packages:
        ivy_dir = f"/tmp/ivy_{os.getpid()}"
        builder = (
            builder
            .config("spark.jars.ivy", ivy_dir)
            .config(
                "spark.jars.packages",
                ",".join([
                    "org.postgresql:postgresql:42.7.7",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ]),
            )
        )

    return builder.getOrCreate()

