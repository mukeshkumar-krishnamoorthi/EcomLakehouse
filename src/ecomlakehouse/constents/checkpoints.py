CHECKPOINT_CONSTANTS = {
    "BRONZE_INGESTION": {
        "name": "bronze",
        "checkpoint_prefix": "_checkpoints",
        "prefix": "postgres_to_bronze",
        "mode": "json"
    },
    "BRONZE_TO_SILVER": {
        "name": "silver",
        "checkpoint_prefix": "_checkpoints",
        "prefix": "bronze_to_silver",
        "mode": "json"
    }
}
