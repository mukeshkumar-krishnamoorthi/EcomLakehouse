import json
import re
from datetime import UTC, datetime

from ecomlakehouse.constents.checkpoints import CHECKPOINT_CONSTANTS
from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def checkpoint_key(checkpoint: str, table_name: str) -> str:
    config = CHECKPOINT_CONSTANTS[checkpoint]

    name = config["name"]
    checkpoint_prefix = config["checkpoint_prefix"]
    mode = config["mode"]

    return f"{name}/{checkpoint_prefix}/{table_name}.{mode}"


def checkpoint_run_key(checkpoint: str, table_name: str, run_id: str) -> str:
    config = CHECKPOINT_CONSTANTS[checkpoint]

    name = config["name"]
    checkpoint_prefix = config["checkpoint_prefix"]
    mode = config["mode"]

    safe_run_id = re.sub(r"[^a-zA-Z0-9_\-.]", "_", run_id)
    return f"{name}/{checkpoint_prefix}/{table_name}/run_id={safe_run_id}.{mode}"


def get_checkpoint(
    checkpoint: str,
    table_name: str,
    run_id: str | None = None,
) -> dict | None:
    s3 = get_s3_client()
    bucket = get_bucket_name()
    key = (
        checkpoint_run_key(checkpoint, table_name, run_id)
        if run_id
        else checkpoint_key(checkpoint, table_name)
    )

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
    last_processed_key: str | None = None,
    run_id: str | None = None,
    watermark_start: str | None = None,
    watermark_end: str | None = None,
    rows_extracted: int = 0,
    rows_written: int = 0,
    rows_quarantined: int = 0,
    committed_files: list[str] | str | None = None,
    committed_at: str | None = None,
    status: str | None = "success",
) -> None:
    target_watermark = watermark_end or last_processed_key
    if target_watermark is None:
        return

    s3 = get_s3_client()
    bucket = get_bucket_name()
    key = checkpoint_key(checkpoint, table_name)

    if isinstance(committed_files, str):
        committed_file_list = [committed_files]
    elif committed_files is None:
        committed_file_list = []
    else:
        committed_file_list = list(committed_files)

    state = {
        "run_id": run_id,
        "table_name": table_name,
        "last_processed_key": str(target_watermark),
        "watermark_start": str(watermark_start) if watermark_start is not None else None,
        "watermark_end": str(target_watermark),
        "rows_extracted": int(rows_extracted),
        "rows_written": int(rows_written),
        "rows_quarantined": int(rows_quarantined),
        "committed_files": committed_file_list,
        "committed_at": committed_at or datetime.now(UTC).isoformat(),
        "status": status,
    }

    body = json.dumps(state, indent=2).encode("utf-8")

    # 1. Active pointer for fast lookup by subsequent DAG runs
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

    # 2. Historical run-level checkpoint audit record
    if run_id:
        run_key = checkpoint_run_key(checkpoint, table_name, run_id)
        s3.put_object(
            Bucket=bucket,
            Key=run_key,
            Body=body,
            ContentType="application/json",
        )
