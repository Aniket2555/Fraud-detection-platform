# 06 — Hybrid Ensemble Training, MLflow Packaging, Scoring Path, Circuit Breaker

**Starting state:** `ml/training/data_preparation.py` referenced a table
(`gold.reconciled_labeled_transactions`) that no notebook in the entire
pipeline ever produced — Phase 4 training could not run at all.
`ml/serving/score.py` had bare imports that only work as standalone
scripts. The ensemble had never been trained end-to-end even once.

**End state:** full hybrid ensemble (XGBoost + Autoencoder + Isolation
Forest, each isotonic-calibrated, stacked via a logistic-regression
meta-learner) trained on real data with a proper chronological split,
packaged as a single `mlflow.pyfunc` model, loaded back and scored
against held-out test rows, with the circuit breaker and fallback-rules
logic verified.

## Data preparation fixes (bugs #32-33)

`ml/training/data_preparation.py`:
- Pointed at `silver.streaming_transactions` (the real table — see
  `05-feature-engineering.md`) instead of the never-produced
  `gold.reconciled_labeled_transactions`.
- Replaced a calendar-month train/val/test split (meaningless for a
  time-compressed streaming replay that only spans minutes/hours of wall
  clock, not real months) with a proper **70/15/15 chronological split**
  using a `Window`/`row_number()` ordering by event time — preserving the
  point-in-time (PIT) property (no test-set information leaking into
  training) without depending on calendar semantics.

## Training driver

Run via the Databricks Command Execution API (create context, submit,
poll), attached to `batch-etl-dev`. Manually: open
`ml/training/train_supervised.py` (or the equivalent ensemble driver
notebook under `ml/pipelines/`) and Run All.

### XGBoost API fix (bug #34)

The installed XGBoost is 3.2.0. The code's own comment claimed
`early_stopping_rounds` was a valid `.fit()` kwarg "in XGBoost 2.x" —
backwards for the actually-installed version, where it's a **constructor**
parameter:
```python
# Before (fails on XGBoost 3.x):
model = xgb.XGBClassifier(...)
model.fit(X_train, y_train, early_stopping_rounds=20, ...)

# After:
model = xgb.XGBClassifier(..., early_stopping_rounds=20)
model.fit(X_train, y_train, eval_set=[(X_val, y_val)])
```
Also removed the now-invalid `use_label_encoder` parameter. Fixed in both
the Optuna hyperparameter-search objective and the final model fit.

### MLflow nested-run fix (bug #35)

`train_xgboost_supervised()`'s Optuna trials call
`mlflow.start_run(nested=True, ...)`, which requires an already-active
parent run and a set experiment — neither existed when the function was
called standalone. Fixed at the call site (the driver script sets the
experiment and opens the parent run before invoking the function), not
inside the reusable function itself, to keep the function composable.

## Ensemble components trained

1. **XGBoost** (supervised) — Optuna-tuned, PR-AUC-optimized
2. **Autoencoder** (PyTorch, unsupervised) — trimmed-MSE reconstruction
   loss, trained on legitimate-only transactions
3. **Isolation Forest** (unsupervised, scikit-learn)
4. Each component's raw score isotonic-calibrated independently
   (`cal_xgb`, `cal_ae`, `cal_if`)
5. **Meta-learner** — logistic regression stacking the 3 calibrated
   scores into a final `fraud_probability`

A suspiciously perfect PR-AUC (1.0) was observed and flagged honestly as
a likely artifact of the synthetic test-data generation (the ULB
dataset's most-predictive V-columns were deterministically hashed into
`card_id` for entity simulation, effectively leaking the label into an
ID field) — **not** reported as a genuinely production-grade result.

## Packaging as a single MLflow PyFunc model

Component artifacts (XGBoost via `joblib`, the PyTorch autoencoder via
`torch.save`, the scaler/isolation-forest/calibrators/meta-learner all
via `joblib`, feature names as JSON) are all saved locally, then logged
together as one `mlflow.pyfunc.PythonModel`
(`fraud_detection.ml.ensemble.ensemble_model.FraudEnsemblePyFunc`) so a
single `mlflow.pyfunc.load_model()` call reconstructs the whole ensemble
at serving time — exactly mirroring how Azure ML (or any other serving
layer) would load it in production.

```python
import mlflow.pyfunc, joblib, torch, json, os

ARTIFACT_DIR = "/tmp/ensemble_artifacts"
os.makedirs(ARTIFACT_DIR, exist_ok=True)
joblib.dump(xgb_model, f"{ARTIFACT_DIR}/xgb_model.joblib")
torch.save(ae_model, f"{ARTIFACT_DIR}/ae_model.pt")
joblib.dump(ae_scaler, f"{ARTIFACT_DIR}/ae_scaler.joblib")
joblib.dump(iso_model, f"{ARTIFACT_DIR}/iso_forest.joblib")
joblib.dump(cal_xgb, f"{ARTIFACT_DIR}/cal_xgb.joblib")
joblib.dump(cal_ae, f"{ARTIFACT_DIR}/cal_ae.joblib")
joblib.dump(cal_if, f"{ARTIFACT_DIR}/cal_if.joblib")
joblib.dump(meta, f"{ARTIFACT_DIR}/meta_learner.joblib")
with open(f"{ARTIFACT_DIR}/feature_names.json", "w") as f:
    json.dump(feature_cols, f)

mlflow.set_experiment("/fraud-detection-phase4-ensemble")
with mlflow.start_run(run_name="ensemble-pyfunc-packaging"):
    mlflow.pyfunc.log_model(
        name="ensemble_model",
        python_model=FraudEnsemblePyFunc(),
        artifacts={
            "xgb_model": f"{ARTIFACT_DIR}/xgb_model.joblib",
            "ae_model": f"{ARTIFACT_DIR}/ae_model.pt",
            "ae_scaler": f"{ARTIFACT_DIR}/ae_scaler.joblib",
            "iso_forest": f"{ARTIFACT_DIR}/iso_forest.joblib",
            "cal_xgb": f"{ARTIFACT_DIR}/cal_xgb.joblib",
            "cal_ae": f"{ARTIFACT_DIR}/cal_ae.joblib",
            "cal_if": f"{ARTIFACT_DIR}/cal_if.joblib",
            "meta_learner": f"{ARTIFACT_DIR}/meta_learner.joblib",
            "feature_names": f"{ARTIFACT_DIR}/feature_names.json",
            "model_version_tag": f"{ARTIFACT_DIR}/model_version.txt",
        },
    )
    run_id = mlflow.active_run().info.run_id
```

Loaded back and scored exactly as a real deployment would:
```python
loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/ensemble_model")
result = loaded_model.predict(X_test[sample_idx:sample_idx + 1])
# -> {"fraud_probability": ..., "component_scores": {...}, "scoring_mode": "full"}
```

## `ml/serving/score.py` fix (bug #36)

Bare imports (`from shap_explainability import ...`,
`from circuit_breaker import ...`) only resolve when the file is run as a
standalone script from its own directory — broken under normal package
import. Fixed to real package paths:
```python
from fraud_detection.ml.serving.shap_explainability import FraudExplainer
from fraud_detection.ml.serving.circuit_breaker import CircuitBreaker
```

`score.py` is the Azure ML Managed Online Endpoint entry point
(`init()`/`run()`). **Note:** actual Azure ML endpoint deployment is
out of scope for this Free Trial architecture (see
`docs/phase0/upgrade_to_production.md`) — everything up through model
packaging, loading, scoring, SHAP explanation, and fallback logic was
verified by calling `init()`/`run()`-equivalent code paths directly in a
Databricks notebook, not through a real Azure ML endpoint.

## Circuit breaker verification

`fraud_detection.ml.serving.circuit_breaker.CircuitBreaker` implements a
CLOSED → OPEN → HALF_OPEN state machine. Verified directly:

```python
cb = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=1, window_size=5)
assert cb.current_state == "CLOSED"
for _ in range(3):
    cb.record_failure()
assert cb.current_state == "OPEN"
assert cb.should_allow_request() is False   # blocks while OPEN

import time; time.sleep(1.1)
assert cb.should_allow_request() is True    # allows a single probe request
assert cb.current_state == "HALF_OPEN"

cb.record_success()
assert cb.current_state == "CLOSED"         # successful probe closes the circuit

# A second breaker, to confirm a *failed* probe re-opens immediately:
cb2 = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=1, window_size=5)
for _ in range(3):
    cb2.record_failure()
time.sleep(1.1)
cb2.should_allow_request()          # -> HALF_OPEN
cb2.record_failure()                # probe fails
assert cb2.current_state == "OPEN"  # re-opens immediately, doesn't wait another window
```
All assertions passed.

## Fallback rules verification

`_execute_fallback_rules()` (the emergency rule-based path used when the
circuit is open or scoring throws) was checked across amount tiers:

| Amount | fraud_probability | Routing intent |
|---|---|---|
| $100 | 0.05 | Approve low-value |
| $600 | 0.20 | Step-up |
| $1,500 | 0.40 | Step-up |
| $6,000 | 0.85 | Manual review |

Response always carries `"scoring_mode": "rules_only_fallback"` so
downstream systems can distinguish real model scores from the emergency
path — never silently approved as if it were a genuine model decision.

## To reproduce this from scratch

1. Ensure the Phase 3 feature store (`05-feature-engineering.md`) is
   materialized.
2. Run `ml/training/data_preparation.py` to produce the chronological
   train/val/test split.
3. Run the ensemble training driver (trains all 3 components + 3
   calibrators + meta-learner, logs everything to
   `/fraud-detection-phase4-ensemble`).
4. Package as a single `mlflow.pyfunc` model (code above) and note the
   `run_id`.
5. Load it back with `mlflow.pyfunc.load_model()` and score a few known
   fraud/legit test rows to sanity-check.
6. Run the circuit-breaker and fallback-rules assertions above.
7. (Not done this session, deliberately out of scope) Deploy to an Azure
   ML Managed Online Endpoint for real HTTP-served inference.
