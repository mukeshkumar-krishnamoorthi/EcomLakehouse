import os

from pyspark.sql import SparkSession


def get_spark():
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9020")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minio_admin")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minio_secret")

    builder = (
        SparkSession.builder
        .appName('EcomLakehouse')
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.postgresql:postgresql:42.7.7",
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                ]
            ),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    return builder.getOrCreate()

