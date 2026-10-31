
import json

from ecomlakehouse.constents.checkpoints import CHECKPOINT_CONSTANTS
from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def checkpoint_key(checkpoint: str, table_name: str) -> str:
    config = CHECKPOINT_CONSTANTS[checkpoint]

    name = config["name"]
    checkpoint_prefix = config["checkpoint_prefix"]
    prefix = config["prefix"]
    mode = config["mode"]

    return f"{name}/{checkpoint_prefix}/{table_name}.{mode}"


def get_checkpoint(checkpoint: str, table_name: str) -> str | None:
    s3 = get_s3_client()
    bucket = get_bucket_name()
    key = checkpoint_key(checkpoint, table_name)

    try:
        response = s3.get_object(
            Bucket=bucket,
            Key=key,
        )
    except Exception as exc:
        # Only treat a genuinely missing object as no checkpoint.
        error_code = (
            exc.response.get("Error", {}).get("Code")
            if hasattr(exc, "response")
            else None
        )

        if error_code in ("NoSuchKey", "404", "NotFound"):
            return None

        raise

    state = json.loads(response["Body"].read())
    return state


def save_checkpoint(
    checkpoint: str,
    table_name: str,
    last_processed_key: str | None,
    status: str | None = "success",
) -> None:
    if last_processed_key is None:
        return

    s3 = get_s3_client()
    bucket = get_bucket_name()
    key = checkpoint_key(checkpoint, table_name)

    state = {
        "table_name": table_name,
        "last_processed_key": last_processed_key,
        "status": status
    }

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )
