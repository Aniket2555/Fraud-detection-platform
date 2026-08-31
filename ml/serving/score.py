"""
Inference Script for Azure ML Managed Online Endpoint.

Production fixes applied:
- model_version loaded from MLflow model metadata at init() — not hardcoded "v1"
- Circuit breaker integrated at the run() call site
- Feature vector shape and NaN validation at score time
- Structured JSON logging with transaction IDs
- Fallback rules clearly labelled as emergency-only (not silent approval)
"""

import json
import os
import time
import logging

import numpy as np
import mlflow.pyfunc

from fraud_detection.ml.serving.shap_explainability import FraudExplainer
from fraud_detection.ml.serving.circuit_breaker import CircuitBreaker

logger = logging.getLogger("fraud_scoring")

model = None
explainer = None
feature_schema: list = []
model_version: str = "unknown"

# Single shared circuit breaker for the model endpoint
_circuit_breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout_sec=30,
    window_size=10,
)


def _resolve_model_path(base_dir: str) -> str:
    """
    Azure ML mounts a registered model under AZUREML_MODEL_DIR/<model-name>/<version>/...,
    where the trailing structure mirrors whatever was registered (here, the MLflow
    artifact_path="ensemble_model" folder logged in ml/pipelines/training_pipeline.py).
    Hardcoding that nested path is fragile — it drifts the moment the model/artifact
    name changes — so instead we search for the directory actually containing the
    model (identified by its MLmodel file) under the mounted root.
    """
    for root, _dirs, files in os.walk(base_dir):
        if "MLmodel" in files:
            return root
    raise FileNotFoundError(f"No MLflow model (MLmodel file) found under {base_dir}")


def init():
    """
    Azure ML initialisation hook — called once when the endpoint pod starts.
    Loads the MLflow PyFunc model, reads model_version from tags/run metadata,
    and initialises the SHAP explainer.
    """
    global model, explainer, feature_schema, model_version

    model_path = _resolve_model_path(os.environ.get("AZUREML_MODEL_DIR", "."))

    logger.info("Loading model from path: %s", model_path)
    model = mlflow.pyfunc.load_model(model_path)

    # Load feature schema from PyFunc model internals
    feature_schema = getattr(
        getattr(model, "_model_impl", None),
        "python_model",
        None,
    )
    if feature_schema is not None:
        feature_schema = getattr(feature_schema, "feature_names", [])
    else:
        feature_schema = []

    # Load model_version from MLflow model metadata (run tags preferred over hardcoded)
    try:
        client = mlflow.tracking.MlflowClient()
        # model_uri is stored in the MLmodel file as metadata
        mlmodel_meta = model.metadata
        run_id = getattr(mlmodel_meta, "run_id", None)
        if run_id:
            run = client.get_run(run_id)
            model_version = run.data.tags.get("model_version",
                                               run.data.params.get("model_version", run_id[:8]))
        else:
            model_version = getattr(mlmodel_meta, "model_uuid", "unknown")[:8]
    except Exception as exc:
        logger.warning("Could not read model_version from MLflow metadata: %s", exc)
        model_version = "unknown"

    logger.info("Model loaded successfully. version=%s features=%d", model_version, len(feature_schema))

    # Initialise SHAP explainer on the XGBoost sub-model
    try:
        raw_xgb = model._model_impl.python_model.xgb_model
        explainer = FraudExplainer(raw_xgb, feature_schema)
        logger.info("SHAP explainer initialised.")
    except Exception as exc:
        logger.warning("SHAP explainer init failed — risk factors will be empty: %s", exc)
        explainer = None


def run(raw_json: str) -> str:
    """
    Azure ML scoring entry point.
    Returns a JSON string with fraud probability, component scores, top risk factors,
    latency, and the resolved model version.
    """
    start_time = time.monotonic()
    transaction_id = "unknown"

    try:
        payload = json.loads(raw_json)
        transaction = payload["transaction"]
        transaction_id = transaction.get("transaction_id", "unknown")
        online_features = payload.get("features", {})

        # --- Circuit breaker guard ---
        if not _circuit_breaker.should_allow_request():
            logger.warning("Circuit OPEN — routing to fallback. txn=%s", transaction_id)
            return _execute_fallback_rules(transaction, "circuit_breaker_open", start_time)

        # --- Build and validate feature vector ---
        feature_vector = _build_feature_vector(transaction, online_features, feature_schema)

        # --- Score ---
        score_result = model.predict(feature_vector)
        _circuit_breaker.record_success()

        fraud_prob = float(score_result["fraud_probability"])

        # --- SHAP explanations (only for suspicious scores to stay within latency SLA) ---
        reasons = []
        if fraud_prob >= 0.30 and explainer is not None:
            try:
                reasons = explainer.get_top_k_reasons(feature_vector, top_k=5)
            except Exception as exc:
                logger.warning("SHAP explanation failed — omitting. txn=%s error=%s",
                               transaction_id, exc)

        latency_ms = (time.monotonic() - start_time) * 1000.0
        logger.info("Scored txn=%s fraud_prob=%.4f latency_ms=%.2f model_version=%s",
                    transaction_id, fraud_prob, latency_ms, model_version)

        response = {
            "transaction_id": transaction_id,
            "fraud_probability": round(fraud_prob, 4),
            "component_scores": score_result.get("component_scores", {}),
            "top_risk_factors": reasons,
            "scoring_mode": score_result.get("scoring_mode", "full"),
            "model_version": model_version,
            "latency_ms": round(latency_ms, 2),
        }
        return json.dumps(response)

    except Exception as exc:
        _circuit_breaker.record_failure()
        logger.error("Scoring error — txn=%s error=%s", transaction_id, exc, exc_info=True)
        try:
            txn = json.loads(raw_json).get("transaction", {})
        except Exception:
            txn = {}
        return _execute_fallback_rules(txn, str(exc), start_time)


def _build_feature_vector(txn: dict, online_feats: dict, schema: list) -> np.ndarray:
    """
    Constructs a numeric feature vector aligned to the model's expected feature schema.
    Missing features are imputed with 0.0. NaN/Inf values are replaced with 0.0.
    """
    vector = []
    for feature_name in schema:
        raw_val = txn.get(feature_name, online_feats.get(feature_name, 0.0))
        try:
            val = float(raw_val)
            if not np.isfinite(val):
                val = 0.0
        except (TypeError, ValueError):
            val = 0.0
        vector.append(val)

    arr = np.array(vector, dtype=np.float32).reshape(1, -1)

    # Final guard: replace any remaining NaN/Inf
    if not np.all(np.isfinite(arr)):
        logger.warning("Non-finite values detected in feature vector — replacing with 0.0. txn=%s",
                       txn.get("transaction_id", "unknown"))
        arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)

    return arr


def _execute_fallback_rules(txn: dict, error_msg: str, start_time: float) -> str:
    """
    Emergency rule-based fallback. Activates ONLY when the ML model is unavailable.
    Scores are conservative (biased towards caution on high-value transactions).
    Response is clearly labelled 'rules_only_fallback' so downstream systems can track it.
    """
    latency_ms = (time.monotonic() - start_time) * 1000.0
    try:
        amount = float(txn.get("amount", 0.0))
    except (TypeError, ValueError):
        amount = 0.0

    if amount > 5000.0:
        score = 0.85   # Route to manual review
    elif amount > 1000.0:
        score = 0.40   # Route to step-up
    elif amount > 500.0:
        score = 0.20   # Route to step-up
    else:
        score = 0.05   # Approve low-value

    return json.dumps({
        "transaction_id": txn.get("transaction_id", "unknown"),
        "fraud_probability": score,
        "component_scores": {},
        "top_risk_factors": [],
        "scoring_mode": "rules_only_fallback",
        "model_version": "fallback",
        "fallback_reason": error_msg,
        "latency_ms": round(latency_ms, 2),
    })
