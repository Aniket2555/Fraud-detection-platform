# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Model Retraining Pipeline
# MAGIC Retrains full ensemble (XGBoost + Autoencoder + IF + Calibrators + Meta-Learner)
# MAGIC on the latest reconciled labeled dataset with PIT-joined features.
# MAGIC
# MAGIC Production fixes applied:
# MAGIC - Artifacts saved to DBFS (dbfs:/tmp/challenger_artifacts/) — accessible from ALL cluster nodes
# MAGIC   (not /tmp/ on driver only)
# MAGIC - Added minimum training data size guard (raises error if < 1000 labeled samples)
# MAGIC - Added fraud rate guard (skips retraining if < 0.1% or > 30% fraud rate — data quality issue)
# MAGIC - challenger_run_id scoped outside try-block to prevent NameError in gate invocation

import mlflow
import mlflow.pyfunc
import numpy as np
import torch
import joblib
import json
import os

from sklearn.metrics import average_precision_score

# ---------------------------------------------------------------------------
dbutils.widgets.text("trigger_reason", "SCHEDULED_BIWEEKLY")
trigger_reason = dbutils.widgets.get("trigger_reason")
print(f"Starting Retraining Pipeline. Trigger: {trigger_reason}")

# ---------------------------------------------------------------------------
# 1. Load high-confidence reconciled labels (confidence >= 0.7)
# ---------------------------------------------------------------------------
labeled_df = spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions").filter(
    "is_fraud_reconciled IS NOT NULL AND label_confidence >= 0.7"
)

total_records = labeled_df.count()
fraud_count = labeled_df.filter("is_fraud_reconciled = 1").count()
fraud_rate = fraud_count / max(1, total_records)

print(f"Labeled dataset: {total_records:,} records, fraud_rate={fraud_rate:.4f}")

# Data quality guards
if total_records < 1000:
    raise ValueError(
        f"Insufficient training data: only {total_records} labeled records. "
        "Need >= 1000. Retraining aborted."
    )

if fraud_rate < 0.001 or fraud_rate > 0.30:
    raise ValueError(
        f"Suspicious fraud rate {fraud_rate:.4f} (expected 0.1%-30%). "
        "Possible data pipeline issue. Retraining aborted."
    )

# ---------------------------------------------------------------------------
# 2. PIT feature join
# ---------------------------------------------------------------------------
from pyspark.sql.functions import col as _col
from fraud_detection.features.point_in_time_join import multi_entity_pit_join

# Daily/hourly-refreshed feature families need a longer PIT lookback than
# the default 1-day used for velocity windows, to tolerate a missed run
# without nulling out an otherwise-available feature (see
# ml/training/data_preparation.py for the same convention).
BASELINE_LOOKBACK_SECONDS = 3 * 24 * 3600
GRAPH_LOOKBACK_SECONDS = 3 * 24 * 3600

card_velocity = spark.table("fraud_detection_dev.gold.feature_card_velocity")
cust_velocity = spark.table("fraud_detection_dev.gold.feature_customer_velocity")
geo_velocity = spark.table("fraud_detection_dev.gold.feature_geo_velocity")
baselines = spark.table("fraud_detection_dev.gold.customer_behavioral_baselines")
merchant_risk = spark.table("fraud_detection_dev.gold.merchant_risk_baselines")
graph_metrics_customer = (
    spark.table("fraud_detection_dev.gold.graph_entity_metrics")
    .filter(_col("entity_type") == "customer")
    .select(
        _col("entity_id").alias("customer_id"),
        "graph_pagerank_score", "graph_degree_centrality", "graph_community_id",
        "feature_timestamp",
    )
)

# NOTE: this previously only joined the 3 velocity/geo families even though
# feature_cols below already selected for base_/merch_/graph_ prefixes too --
# those columns simply never existed in enriched_df, so retraining silently
# trained on fewer features than the initial Phase 4 training pipeline
# (ml/training/data_preparation.py). Now joins all 6 families, matching it.
feature_specs = [
    (card_velocity, "card_id"),
    (cust_velocity, "customer_id"),
    (geo_velocity, "card_id"),
    (baselines, "customer_id", BASELINE_LOOKBACK_SECONDS),
    (merchant_risk, "merchant_id", BASELINE_LOOKBACK_SECONDS),
    (graph_metrics_customer, "customer_id", GRAPH_LOOKBACK_SECONDS),
]
enriched_df = multi_entity_pit_join(labeled_df, feature_specs)

feature_cols = [c for c in enriched_df.columns if c.startswith((
    "feature_", "vel_", "geo_", "base_", "merch_", "graph_"
))]

print(f"Feature engineering complete. n_features={len(feature_cols)}")

# ---------------------------------------------------------------------------
# 3. Temporal train/val/test split (no shuffle — preserves time order)
# ---------------------------------------------------------------------------
pdf = enriched_df.select(feature_cols + ["is_fraud_reconciled", "event_date"]).toPandas()
pdf = pdf.sort_values("event_date").reset_index(drop=True)
n = len(pdf)

train_pdf = pdf.iloc[:int(n * 0.70)]
val_pdf   = pdf.iloc[int(n * 0.70):int(n * 0.85)]
test_pdf  = pdf.iloc[int(n * 0.85):]

X_train, y_train = train_pdf[feature_cols].values, train_pdf["is_fraud_reconciled"].values
X_val,   y_val   = val_pdf[feature_cols].values,   val_pdf["is_fraud_reconciled"].values
X_test,  y_test  = test_pdf[feature_cols].values,  test_pdf["is_fraud_reconciled"].values

print(f"Split: train={len(y_train):,}, val={len(y_val):,}, test={len(y_test):,}")

# ---------------------------------------------------------------------------
# 4. Training — all artifacts go to DBFS (accessible from all cluster nodes)
# ---------------------------------------------------------------------------

# DBFS path is accessible from both driver and all worker nodes
ARTIFACTS_DBFS = "dbfs:/tmp/challenger_artifacts/"
ARTIFACTS_LOCAL = "/dbfs/tmp/challenger_artifacts/"  # POSIX equivalent for file I/O
os.makedirs(ARTIFACTS_LOCAL, exist_ok=True)

challenger_run_id = None  # Defined outside try so gate call always has access

mlflow.set_experiment("/Shared/fraud_detection_retraining")

with mlflow.start_run(run_name=f"challenger_{trigger_reason}") as run:
    challenger_run_id = run.info.run_id

    mlflow.log_param("trigger_reason", trigger_reason)
    mlflow.log_param("train_size", len(y_train))
    mlflow.log_param("fraud_rate_train", round(float(np.mean(y_train)), 4))
    mlflow.log_param("n_features", len(feature_cols))

    # -- XGBoost (Optuna tuned) --
    from fraud_detection.ml.training.train_supervised import train_xgboost_supervised
    xgb_model = train_xgboost_supervised(
        X_train, y_train, X_val, y_val, feature_cols, n_trials=30
    )

    # -- Autoencoder (trained on legitimate transactions only) --
    from fraud_detection.ml.training.models.autoencoder import train_autoencoder
    X_legit = X_train[y_train == 0]
    ae_model, ae_scaler = train_autoencoder(X_legit, input_dim=len(feature_cols), epochs=30)

    # -- Isolation Forest --
    from fraud_detection.ml.training.train_isolation_forest import train_isolation_forest, get_isolation_scores
    iso_model = train_isolation_forest(X_legit)

    # -- Isotonic Calibrators --
    from fraud_detection.ml.training.calibrate_model import ScoreCalibrator

    raw_xgb_val = xgb_model.predict_proba(X_val)[:, 1]
    scaled_val  = ae_scaler.transform(X_val)
    with torch.no_grad():
        raw_ae_val = ae_model.reconstruction_error(
            torch.tensor(scaled_val, dtype=torch.float32)
        )
    raw_if_val = get_isolation_scores(iso_model, X_val)

    cal_xgb = ScoreCalibrator("isotonic").fit(raw_xgb_val, y_val)
    cal_ae  = ScoreCalibrator("isotonic").fit(raw_ae_val, y_val)
    cal_if  = ScoreCalibrator("isotonic").fit(raw_if_val, y_val)

    # -- Stacking Meta-Learner --
    from fraud_detection.ml.training.train_meta_learner import StackingMetaLearner
    meta = StackingMetaLearner()
    meta.fit(
        cal_xgb.transform(raw_xgb_val),
        cal_ae.transform(raw_ae_val),
        cal_if.transform(raw_if_val),
        y_val,
    )

    # -- Evaluate on held-out test set --
    raw_xgb_test = xgb_model.predict_proba(X_test)[:, 1]
    scaled_test  = ae_scaler.transform(X_test)
    with torch.no_grad():
        raw_ae_test = ae_model.reconstruction_error(
            torch.tensor(scaled_test, dtype=torch.float32)
        )
    raw_if_test = get_isolation_scores(iso_model, X_test)

    final_probs = meta.predict_proba(
        cal_xgb.transform(raw_xgb_test),
        cal_ae.transform(raw_ae_test),
        cal_if.transform(raw_if_test),
    )
    test_pr_auc = float(average_precision_score(y_test, final_probs))
    mlflow.log_metric("test_pr_auc", test_pr_auc)
    print(f"Challenger test PR-AUC: {test_pr_auc:.4f}")

    # -- Persist artifacts to DBFS --
    joblib.dump(xgb_model,  f"{ARTIFACTS_LOCAL}/xgb_model.pkl")
    torch.save(ae_model,    f"{ARTIFACTS_LOCAL}/ae_model.pt")
    joblib.dump(ae_scaler,  f"{ARTIFACTS_LOCAL}/ae_scaler.pkl")
    joblib.dump(iso_model,  f"{ARTIFACTS_LOCAL}/iso_forest.pkl")
    joblib.dump(cal_xgb,    f"{ARTIFACTS_LOCAL}/cal_xgb.pkl")
    joblib.dump(cal_ae,     f"{ARTIFACTS_LOCAL}/cal_ae.pkl")
    joblib.dump(cal_if,     f"{ARTIFACTS_LOCAL}/cal_if.pkl")
    joblib.dump(meta,       f"{ARTIFACTS_LOCAL}/meta_learner.pkl")

    with open(f"{ARTIFACTS_LOCAL}/feature_names.json", "w") as f:
        json.dump(feature_cols, f)

    # Write model_version tag so PyFunc wrapper can read it
    with open(f"{ARTIFACTS_LOCAL}/model_version_tag.txt", "w") as f:
        f.write(challenger_run_id[:8])

    # -- Log PyFunc model to MLflow --
    from fraud_detection.ml.ensemble.ensemble_model import FraudEnsemblePyFunc
    artifact_map = {
        "xgb_model":        f"{ARTIFACTS_LOCAL}/xgb_model.pkl",
        "ae_model":         f"{ARTIFACTS_LOCAL}/ae_model.pt",
        "ae_scaler":        f"{ARTIFACTS_LOCAL}/ae_scaler.pkl",
        "iso_forest":       f"{ARTIFACTS_LOCAL}/iso_forest.pkl",
        "cal_xgb":          f"{ARTIFACTS_LOCAL}/cal_xgb.pkl",
        "cal_ae":           f"{ARTIFACTS_LOCAL}/cal_ae.pkl",
        "cal_if":           f"{ARTIFACTS_LOCAL}/cal_if.pkl",
        "meta_learner":     f"{ARTIFACTS_LOCAL}/meta_learner.pkl",
        "feature_names":    f"{ARTIFACTS_LOCAL}/feature_names.json",
        "model_version_tag": f"{ARTIFACTS_LOCAL}/model_version_tag.txt",
    }
    mlflow.pyfunc.log_model(
        artifact_path="ensemble_model",
        python_model=FraudEnsemblePyFunc(),
        artifacts=artifact_map,
    )

    print(f"Challenger logged to MLflow. Run ID: {challenger_run_id}")

# ---------------------------------------------------------------------------
# 5. Invoke Champion-Challenger gate (outside the MLflow run context)
# ---------------------------------------------------------------------------
if challenger_run_id is None:
    raise RuntimeError("challenger_run_id not set — retraining run did not complete.")

dbutils.notebook.run(
    "/Repos/fraud-detection/databricks/notebooks/mlops/champion_challenger_gate",
    timeout_seconds=1800,
    arguments={"challenger_run_id": challenger_run_id},
)
