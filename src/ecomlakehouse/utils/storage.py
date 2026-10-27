import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv

try:
    from airflow.sdk import Variable
except ImportError:
    Variable = None

load_dotenv()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def get_bucket_name():
    """
    Get the S3 bucket name from Airflow variables or environment.
    """
    bucket_name = None
    if Variable is not None:
        try:
            bucket_name = Variable.get("BUCKET")
        except Exception:
            bucket_name = None

    if bucket_name is None:
        bucket_name = os.getenv("S3_BUCKET", "ecommerce-lake")

    return bucket_name
