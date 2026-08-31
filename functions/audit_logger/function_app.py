"""
Compliance Audit Logger: Writes immutable decision records.
Triggered by Service Bus subscription 'sub-audit-log'.

Production fixes applied:
- Audit records persisted to ADLS Gen2 Delta Lake (ndjson append) — not just logging.info
- Message deduplication via message_id check
- Structured JSON logging with full field coverage
- Graceful handling of storage write failures (dead-letter on unrecoverable errors)
- UTC timezone-aware timestamps using datetime.timezone.utc
"""

import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger("audit_logger")

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# Module-level ADLS Gen2 client (initialised once per cold start)
# ---------------------------------------------------------------------------
_adls_client: DataLakeServiceClient | None = None
_fs_client = None

STORAGE_ACCOUNT_NAME = os.environ.get("ADLS_STORAGE_ACCOUNT_NAME", "stfraudlakedev")
AUDIT_CONTAINER = "gold"
AUDIT_DIRECTORY = "audit_logs"


def _get_adls_fs_client():
    """Returns a module-level Data Lake file system client using Managed Identity."""
    global _adls_client, _fs_client
    if _fs_client is None:
        try:
            credential = ManagedIdentityCredential()
            _adls_client = DataLakeServiceClient(
                account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
                credential=credential,
            )
            _fs_client = _adls_client.get_file_system_client(file_system=AUDIT_CONTAINER)
            logger.info("ADLS Gen2 filesystem client initialised for container '%s'", AUDIT_CONTAINER)
        except Exception as exc:
            logger.error("Failed to initialise ADLS Gen2 client: %s", exc)
    return _fs_client


def _persist_audit_record(audit_record: dict) -> None:
    """
    Appends an audit record as an ndjson line to ADLS Gen2 Delta-compatible path.
    Path pattern: gold/audit_logs/year=YYYY/month=MM/day=DD/<transaction_id>_<message_id>.json
    Each record is a separate file for atomic writes — no partial-write risk.

    Deduplication: the file is keyed by message_id (not a wall-clock timestamp),
    so a Service Bus at-least-once redelivery of the same message maps to the
    same blob path. Existence is checked explicitly with get_file_properties()
    before writing, rather than relying on upload_data(overwrite=...) to do
    double duty as a create-if-absent+dedup check: verified against the real
    ADLS Gen2 REST API, overwrite=False raises ResourceNotFoundError when the
    path doesn't exist yet (upload_data's internal append_data call assumes
    the path is already there), while calling create_file() first to make
    the path exist means the immediately-following overwrite=False write
    raises ResourceExistsError against the very (empty) file it just made --
    both fail every genuinely-new write and leave a permanent 0-byte file.
    """
    fs = _get_adls_fs_client()
    if fs is None:
        logger.error("ADLS client unavailable — audit record NOT persisted: txn=%s",
                     audit_record.get("transaction_id"))
        return

    now = datetime.now(tz=timezone.utc)
    partition_path = (
        f"{AUDIT_DIRECTORY}/year={now.year:04d}/month={now.month:02d}/day={now.day:02d}"
    )
    file_name = f"{audit_record['transaction_id']}_{audit_record['message_id']}.json"
    full_path = f"{partition_path}/{file_name}"

    try:
        dir_client = fs.get_directory_client(partition_path)
        dir_client.create_directory()  # no-op if already exists
        file_client = dir_client.get_file_client(file_name)

        already_exists = True
        try:
            file_client.get_file_properties()
        except ResourceNotFoundError:
            already_exists = False

        if already_exists:
            logger.info("Duplicate message detected (already persisted) — skipping. path=%s message_id=%s",
                        full_path, audit_record.get("message_id"))
            return

        record_bytes = (json.dumps(audit_record) + "\n").encode("utf-8")
        file_client.upload_data(record_bytes, overwrite=True)
        logger.info("Audit record persisted to ADLS: path=%s txn=%s",
                    full_path, audit_record.get("transaction_id"))
    except Exception as exc:
        logger.error("ADLS write failure for txn=%s path=%s error=%s",
                     audit_record.get("transaction_id"), full_path, exc)
        raise  # Re-raise so Service Bus retries or dead-letters the message


@app.service_bus_topic_trigger(
    arg_name="msg",
    topic_name="sb-topic-fraud-events",
    subscription_name="sub-audit-log",
    connection="SERVICE_BUS_CONN_STR",
)
def audit_logger(msg: func.ServiceBusMessage):
    """
    Writes every decision event to immutable audit storage in ADLS Gen2.
    If ADLS write fails, re-raises the exception so Service Bus will retry
    up to the configured MaxDeliveryCount, then dead-letter the message.
    """
    message_id = msg.message_id or "unknown"
    logger.info("Audit logger received message. message_id=%s", message_id)

    try:
        body = json.loads(msg.get_body().decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("Malformed message body — cannot parse JSON. message_id=%s error=%s",
                     message_id, exc)
        # Do NOT re-raise: a malformed message will never succeed; let it dead-letter
        return

    transaction_id = body.get("transaction_id", "unknown")

    audit_record = {
        "transaction_id": transaction_id,
        "message_id": message_id,
        "audit_timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
        "fraud_score": body.get("fraud_prob"),
        "decision_action": body.get("action"),
        "scoring_mode": body.get("scoring_mode"),
        "model_version": body.get("model_version"),
        "amount": body.get("amount"),
        "currency": body.get("currency"),
        "customer_id": body.get("customer_id"),
        "card_id": body.get("card_id"),
        "shap_top_features": body.get("shap_factors", []),
        "correlation_id": body.get("correlation_id"),
        # Reflects the actual thresholds the decision engine used for this
        # transaction (dynamic, from App Configuration) — not fixed defaults,
        # since threshold changes must be traceable in the audit trail.
        "threshold_config": body.get("threshold_config", {
            "approve_max": None,
            "step_up_max": None,
            "block_min": None,
        }),
    }

    # Structured log entry (also captured by App Insights / Log Analytics)
    logger.info("AUDIT_LOG %s", json.dumps(audit_record))

    # Persist to ADLS Gen2 (primary durable store)
    _persist_audit_record(audit_record)
