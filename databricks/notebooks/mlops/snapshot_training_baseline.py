# Databricks notebook source
# MAGIC %md
# MAGIC # Snapshot Training Baseline (Phase 6 prerequisite)
# MAGIC `run_daily_drift_check.py` needs a reference feature distribution
# MAGIC (`gold.train_feature_snapshot`) and a reference prediction-score distribution
# MAGIC (`gold.train_prediction_baseline`) to compare today's live traffic against.
# MAGIC Neither was ever created anywhere in Phase 3/4/5 -- this notebook builds both,
# MAGIC one time, from the same PIT-joined feature set the registered Champion model
# MAGIC was actually trained on (`ml.training.data_preparation.prepare_training_dataset`),
# MAGIC so the "reference" distribution is genuinely what training saw, not a guess.

import mlflow
from fraud_detection.ml.training.data_preparation import prepare_training_dataset

train_df, val_df, test_df = prepare_training_dataset(spark)

# Must match the registered Champion's actual training schema exactly -- see the
# same fix (and its rationale) in retrain_pipeline.py and champion_challenger_gate.py.
RAW_FEATURE_COLS = ["amount", "latitude", "longitude"]
EXCLUDED_TIMESTAMP_COLS = {"base_cust_first_seen_ts", "base_cust_last_seen_ts"}
feature_cols = RAW_FEATURE_COLS + [
    c for c in train_df.columns
    if c.startswith(("feature_", "vel_", "geo_", "base_", "merch_", "graph_"))
    and c not in EXCLUDED_TIMESTAMP_COLS
]
print(f"n_features={len(feature_cols)}")

# --- gold.train_feature_snapshot: the reference distribution drift checks compare against ---
(
    train_df.select(feature_cols)
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.train_feature_snapshot")
)
print(f"Wrote {train_df.count()} rows to gold.train_feature_snapshot")

# --- gold.train_prediction_baseline: score the held-out test split with the registered Champion ---
mlflow.set_registry_uri("databricks-uc")
champion = mlflow.pyfunc.load_model("models:/fraud_detection_dev.gold.fraud_ensemble_champion@champion")

# Cap the baseline sample size -- this table is a reference distribution for drift
# comparison, not a full evaluation set, and the PyFunc wrapper's predict() only scores
# one row at a time (see ml/ensemble/ensemble_model.py), so scoring the entire test
# split row-by-row would be needlessly slow.
BASELINE_SAMPLE_SIZE = 1000
test_pdf = test_df.select(feature_cols).limit(BASELINE_SAMPLE_SIZE).toPandas()
import numpy as np
X_test = test_pdf[feature_cols].values.astype(np.float32)

scores = [float(champion.predict(X_test[i:i + 1])["fraud_probability"]) for i in range(len(X_test))]

baseline_df = spark.createDataFrame([(s,) for s in scores], ["fraud_probability"])
(
    baseline_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.train_prediction_baseline")
)
print(f"Wrote {len(scores)} rows to gold.train_prediction_baseline. "
      f"mean={np.mean(scores):.4f} std={np.std(scores):.4f}")
