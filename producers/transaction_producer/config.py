"""
Producer configuration dataclass reading environment variables.
"""
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone


def _env_bool(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class ProducerConfig:
    eventhub_conn_str: str = field(
        default_factory=lambda: os.environ.get("EVENTHUB_PRODUCER_CONN_STR", "")
    )
    eventhub_name: str = field(
        default_factory=lambda: os.environ.get("EVENTHUB_NAME", "eh-transactions")
    )
    speed_multiplier: float = float(os.environ.get("SPEED_MULTIPLIER", "100"))
    batch_size: int = int(os.environ.get("BATCH_SIZE", "100"))
    max_events: int = int(os.environ.get("MAX_EVENTS", "0"))
    dataset_path: str = os.environ.get("DATASET_PATH", "data/creditcard.csv")
    schema_path: str = os.environ.get("SCHEMA_PATH", "schemas/transaction_event_v1.json")
    validate_schema: bool = field(default_factory=lambda: _env_bool("VALIDATE_SCHEMA", "true"))
    log_interval: int = int(os.environ.get("LOG_INTERVAL", "1000"))
    # Anchors synthesized event_time = time_anchor + dataset's Time offset.
    # Defaults to "now" so a live replay produces realistic near-real-time
    # timestamps (and therefore a meaningful enqueuedTime - event_time late-
    # arrival distribution); override with a fixed value for reproducible
    # historical-looking test data instead.
    time_anchor: str = field(
        default_factory=lambda: os.environ.get(
            "TIME_ANCHOR", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    )
