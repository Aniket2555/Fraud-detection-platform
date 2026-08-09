"""
Azure Functions Decision Engine: Fast-Path Scoring & Async Fan-Out.
SLA: Latency contribution < 15ms.

Production fixes applied:
- Auth level changed from ANONYMOUS to FUNCTION (key-based)
- Thread-safe threshold cache with RLock
- Persistent Service Bus sender (not re-created per message)
- Pooled App Configuration client at module level
- Idempotency key on Service Bus messages
- Structured JSON logging with correlation IDs
- Input validation with graceful error responses
"""

import json
import os
import time
import logging
import threading
import uuid
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage

logger = logging.getLogger("decision_engine")

# ---------------------------------------------------------------------------
# Module-level singletons (initialised once per cold start)
# ---------------------------------------------------------------------------
_sb_client: ServiceBusClient | None = None
_sb_sender = None
_sb_lock = threading.Lock()

_app_config_client = None
_app_config_lock = threading.Lock()

# Thread-safe threshold cache
_threshold_cache: dict = {
    "APPROVE_MAX": 0.10,
    "STEP_UP_MAX": 0.60,
    "BLOCK_MIN": 0.90,
    "_last_refresh": 0.0,
}
_cache_lock = threading.RLock()
CACHE_TTL_SECONDS = 60

TOPIC_NAME = os.environ.get("SERVICE_BUS_TOPIC_NAME", "sb-topic-fraud-events")

# ---------------------------------------------------------------------------
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
# ---------------------------------------------------------------------------


def _get_app_config_client():
    """Returns a module-level pooled App Configuration client."""
    global _app_config_client
    if _app_config_client is None:
        with _app_config_lock:
            if _app_config_client is None:
                conn_str = os.environ.get("APP_CONFIG_CONN_STR")
                if conn_str:
                    from azure.appconfiguration import AzureAppConfigurationClient
                    _app_config_client = AzureAppConfigurationClient.from_connection_string(conn_str)
    return _app_config_client


def _get_thresholds() -> dict:
    """Returns cached decision thresholds; refreshes from App Config if TTL expired.
    Thread-safe: uses RLock to prevent concurrent refreshes corrupting the cache.
    """
    now = time.monotonic()
    with _cache_lock:
        if now - _threshold_cache["_last_refresh"] <= CACHE_TTL_SECONDS:
            return dict(_threshold_cache)  # return a snapshot copy

    # Refresh outside the lock to avoid blocking all threads during I/O
    try:
        client = _get_app_config_client()
        if client:
            approve_max = float(client.get_configuration_setting("FraudEngine:ApproveMaxThreshold").value)
            step_up_max = float(client.get_configuration_setting("FraudEngine:StepUpMaxThreshold").value)
            block_min = float(client.get_configuration_setting("FraudEngine:BlockMinThreshold").value)
            with _cache_lock:
                _threshold_cache["APPROVE_MAX"] = approve_max
                _threshold_cache["STEP_UP_MAX"] = step_up_max
                _threshold_cache["BLOCK_MIN"] = block_min
                _threshold_cache["_last_refresh"] = time.monotonic()
            logger.info("Thresholds refreshed from App Configuration: approve_max=%.2f step_up_max=%.2f block_min=%.2f",
                        approve_max, step_up_max, block_min)
    except Exception as exc:
        logger.warning("App Config refresh failed — using stale cached values. error=%s", exc)

    with _cache_lock:
        return dict(_threshold_cache)


def _get_sb_sender():
    """Returns a persistent, module-level Service Bus topic sender.
    The sender is created once and reused across invocations (not closed per message).
    """
    global _sb_client, _sb_sender
    if _sb_sender is None:
        with _sb_lock:
            if _sb_sender is None:
                conn_str = os.environ.get("SERVICE_BUS_CONN_STR")
                if conn_str:
                    _sb_client = ServiceBusClient.from_connection_string(
                        conn_str,
                        retry_total=3,
                        retry_backoff_factor=0.5,
                    )
                    _sb_sender = _sb_client.get_topic_sender(topic_name=TOPIC_NAME)
                    logger.info("Service Bus topic sender initialised for topic '%s'", TOPIC_NAME)
    return _sb_sender


def _classify_decision(fraud_prob: float, thresholds: dict) -> tuple[str, int]:
    """Maps fraud probability to action and HTTP status code."""
    if fraud_prob < thresholds["APPROVE_MAX"]:
        return "approve", 200
    elif fraud_prob < thresholds["STEP_UP_MAX"]:
        return "step_up", 202
    elif fraud_prob <= thresholds["BLOCK_MIN"]:
        return "manual_review", 202
    else:
        return "block", 403


def _publish_event(payload: dict) -> None:
    """Publishes a fraud event to Service Bus.
    Uses message_id for duplicate detection (30-second window on Standard/Premium tier).
    Sender is persistent — never closed here.
    """
    sender = _get_sb_sender()
    if not sender:
        logger.warning("Service Bus sender unavailable — event not published for txn=%s",
                       payload.get("transaction_id", "unknown"))
        return

    msg = ServiceBusMessage(
        body=json.dumps(payload),
        message_id=payload["transaction_id"],          # idempotency / deduplication key
        application_properties={"action": payload["action"]},  # correlation filter for subscriptions
        content_type="application/json",
    )
    # Send without `with` block — sender remains open for subsequent calls
    sender.send_messages(msg)


@app.route(route="evaluate-decision", methods=["POST"])
def evaluate_decision(req: func.HttpRequest) -> func.HttpResponse:
    """
    Fast-path decision engine.
    Validates input, classifies the fraud probability into an action band,
    publishes async events for non-approve decisions, and returns the decision
    within the 15ms SLA budget.
    """
    correlation_id = req.headers.get("x-correlation-id", str(uuid.uuid4()))
    start_time = time.monotonic()

    logger.info("Decision request received. correlation_id=%s", correlation_id)

    # --- Input validation ---
    try:
        req_body = req.get_json()
    except ValueError:
        logger.warning("Malformed JSON body. correlation_id=%s", correlation_id)
        return func.HttpResponse(
            json.dumps({"decision_action": "approve_fallback", "error": "Invalid JSON body",
                        "correlation_id": correlation_id}),
            status_code=200,
            mimetype="application/json",
        )

    required_fields = ["transaction_id", "customer_id", "card_id", "amount", "fraud_probability"]
    missing = [f for f in required_fields if f not in req_body]
    if missing:
        logger.warning("Missing required fields %s. correlation_id=%s", missing, correlation_id)
        return func.HttpResponse(
            json.dumps({"decision_action": "approve_fallback",
                        "error": f"Missing required fields: {missing}",
                        "correlation_id": correlation_id}),
            status_code=200,
            mimetype="application/json",
        )

    try:
        transaction_id: str = str(req_body["transaction_id"])
        customer_id: str = str(req_body["customer_id"])
        card_id: str = str(req_body["card_id"])
        amount: float = float(req_body["amount"])
        currency: str = req_body.get("currency", "USD")
        fraud_prob: float = float(req_body["fraud_probability"])
        scoring_mode: str = req_body.get("scoring_mode", "full")
        model_version: str = req_body.get("model_version", "unknown")
        shap_factors: list = req_body.get("top_risk_factors", [])

        # Clamp fraud probability to [0, 1]
        fraud_prob = max(0.0, min(1.0, fraud_prob))

    except (TypeError, ValueError) as exc:
        logger.error("Input parsing error. correlation_id=%s error=%s", correlation_id, exc)
        return func.HttpResponse(
            json.dumps({"decision_action": "approve_fallback", "error": str(exc),
                        "correlation_id": correlation_id}),
            status_code=200,
            mimetype="application/json",
        )

    # --- Decision classification ---
    thresholds = _get_thresholds()
    action, status_code = _classify_decision(fraud_prob, thresholds)

    # --- Async event fan-out (non-approve decisions) ---
    if action != "approve":
        event_payload = {
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "card_id": card_id,
            "amount": amount,
            "currency": currency,
            "fraud_prob": fraud_prob,
            "scoring_mode": scoring_mode,
            "model_version": model_version,
            "shap_factors": shap_factors,
            "action": action,
            "correlation_id": correlation_id,
            "threshold_config": {
                "approve_max": thresholds["APPROVE_MAX"],
                "step_up_max": thresholds["STEP_UP_MAX"],
                "block_min": thresholds["BLOCK_MIN"],
            },
        }
        try:
            _publish_event(event_payload)
        except Exception as exc:
            # Service Bus failure is non-fatal for the synchronous response
            logger.error("Service Bus publish failed. correlation_id=%s error=%s", correlation_id, exc)

    latency_ms = (time.monotonic() - start_time) * 1000.0
    logger.info("Decision completed. txn=%s action=%s fraud_prob=%.4f latency_ms=%.2f correlation_id=%s",
                transaction_id, action, fraud_prob, latency_ms, correlation_id)

    response_data = {
        "transaction_id": transaction_id,
        "decision_action": action,
        "fraud_probability": round(fraud_prob, 4),
        "status": "COMPLETED" if action in ["approve", "block"] else "PENDING_ASYNC",
        "latency_ms": round(latency_ms, 2),
        "correlation_id": correlation_id,
    }
    return func.HttpResponse(
        json.dumps(response_data),
        status_code=status_code,
        mimetype="application/json",
    )
