# Databricks notebook source
# MAGIC %md
# MAGIC # Asynchronous Shadow Scoring Pipeline (Batch Mode)
# MAGIC Scores recent transactions with both Champion and Challenger models.
# MAGIC Results logged to gold.shadow_scoring_logs with MERGE idempotency.
# MAGIC
# MAGIC Production fixes applied:
# MAGIC - Added guard: skips silently if no Staging model is available
# MAGIC   (previously crashed with MlflowException when no Challenger exists)
# MAGIC - pandas_udf signatures corrected: UDFs now accept an iterator of pd.Series
# MAGIC   (not iterator of DataFrames) matching the correct MapIterator pattern
# MAGIC - MERGE INTO replaces append mode — prevents duplicate shadow rows on re-run
# MAGIC - Added error_flag column: -1.0 scores are flagged instead of silently misrepresenting
# MAGIC - Factored model loading into a safe helper with fallback logging
# MAGIC - This workspace's model registry is Unity Catalog (3-level names + aliases), not
# MAGIC   the legacy workspace registry the plan assumed -- switched to
# MAGIC   `models:/fraud_detection_dev.gold.fraud_ensemble_champion@champion` /
# MAGIC   `@challenger` (set by champion_challenger_gate.py on a rejected run).
# MAGIC - Added CREATE TABLE IF NOT EXISTS for the shadow_scoring_logs MERGE target.
# MAGIC - Same PIT-join gap as champion_challenger_gate.py: `gold.reconciled_labeled_transactions`
# MAGIC   only carries raw transaction columns, never the vel_/geo_/base_/merch_/graph_
# MAGIC   engineered features the models actually need -- selecting champion_features/
# MAGIC   challenger_features directly off that table always raised
# MAGIC   UNRESOLVED_COLUMN. Added the same multi_entity_pit_join used by
# MAGIC   retrain_pipeline.py / champion_challenger_gate.py.

import mlflow.pyfunc
import numpy as np
from pyspark.sql.functions import col, current_timestamp
from fraud_detection.features.point_in_time_join import multi_entity_pit_join

mlflow.set_registry_uri("databricks-uc")
CHAMPION_MODEL_NAME = "fraud_detection_dev.gold.fraud_ensemble_champion"

spark.sql("""
    CREATE TABLE IF NOT EXISTS fraud_detection_dev.gold.shadow_scoring_logs (
        transaction_id STRING,
        event_time_ts TIMESTAMP,
        amount DOUBLE,
        is_fraud_reconciled INT,
        champion_score DOUBLE,
        challenger_score DOUBLE,
        score_delta DOUBLE,
        scoring_error BOOLEAN,
        champion_version STRING,
        challenger_version STRING,
        _shadow_scored_at TIMESTAMP
    ) USING DELTA
""")

# ---------------------------------------------------------------------------
# 1. Load Champion (required) and Challenger (optional -- @challenger alias)
# ---------------------------------------------------------------------------

champion_model = mlflow.pyfunc.load_model(f"models:/{CHAMPION_MODEL_NAME}@champion")
champion_features: list = champion_model._model_impl.python_model.feature_names
champion_version: str   = getattr(champion_model._model_impl.python_model, "model_version", "champion")

challenger_model = None
challenger_features: list = champion_features  # default — overridden below if a challenger exists
challenger_version: str   = "no_challenger"

try:
    challenger_model   = mlflow.pyfunc.load_model(f"models:/{CHAMPION_MODEL_NAME}@challenger")
    challenger_features = challenger_model._model_impl.python_model.feature_names
    challenger_version  = getattr(challenger_model._model_impl.python_model, "model_version", "challenger")
    print(f"Challenger model loaded. version={challenger_version}")
except mlflow.exceptions.MlflowException:
    print("No @challenger alias registered — shadow scoring will only score Champion.")

if challenger_model is None:
    print("Skipping shadow scoring: no Challenger model available.")
    dbutils.notebook.exit("NO_CHALLENGER")


# ---------------------------------------------------------------------------
# 2. Load recent transactions for the scoring window
# ---------------------------------------------------------------------------

labeled_recent = spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions").filter(
    "event_date >= current_date() - 1"
)

BASELINE_LOOKBACK_SECONDS = 3 * 24 * 3600
GRAPH_LOOKBACK_SECONDS = 3 * 24 * 3600
card_velocity = spark.table("fraud_detection_dev.gold.feature_card_velocity")
cust_velocity = spark.table("fraud_detection_dev.gold.feature_customer_velocity")
geo_velocity = spark.table("fraud_detection_dev.gold.feature_geo_velocity")
baselines = spark.table("fraud_detection_dev.gold.customer_behavioral_baselines")
merchant_risk = spark.table("fraud_detection_dev.gold.merchant_risk_baselines")
graph_metrics_customer = (
    spark.table("fraud_detection_dev.gold.graph_entity_metrics")
    .filter(col("entity_type") == "customer")
    .select(
        col("entity_id").alias("customer_id"),
        "graph_pagerank_score", "graph_degree_centrality", "graph_community_id",
        "feature_timestamp",
    )
)
feature_specs = [
    (card_velocity, "card_id"),
    (cust_velocity, "customer_id"),
    (geo_velocity, "card_id"),
    (baselines, "customer_id", BASELINE_LOOKBACK_SECONDS),
    (merchant_risk, "merchant_id", BASELINE_LOOKBACK_SECONDS),
    (graph_metrics_customer, "customer_id", GRAPH_LOOKBACK_SECONDS),
]
enriched_recent = multi_entity_pit_join(labeled_recent, feature_specs)

recent_txns = enriched_recent.select(
    # "amount" deliberately excluded here -- it's already part of RAW_FEATURE_COLS
    # inside champion_features/challenger_features (same duplicate-column issue
    # already fixed in champion_challenger_gate.py).
    ["transaction_id", "event_time_ts", "is_fraud_reconciled"]
    + list(set(champion_features + challenger_features))
)

n_txns = recent_txns.count()
print(f"Shadow scoring window: {n_txns:,} transactions.")

if n_txns == 0:
    print("No transactions in scoring window — exiting.")
    dbutils.notebook.exit("NO_DATA")


# ---------------------------------------------------------------------------
# 3. Score both models (plain per-row loop, not pandas_udf)
# ---------------------------------------------------------------------------
# A pandas_udf's Iterator[Series]->Iterator[Series] contract yields ONE Series per
# batch only when the UDF has a single input column; with 30 (one per feature),
# each batch item is actually a *tuple* of Series, which the previous
# `for val in batch_series` loop didn't account for -- it crashed with
# "takes 1 positional argument but 30 were given" the moment Spark tried to call
# it. FraudEnsemblePyFunc has no true batch predict() path either way (see
# champion_challenger_gate.py), so there's no vectorization to gain here on a
# single-node cluster -- collect a capped sample to the driver and score in a
# plain loop instead, matching the reliable pattern already used elsewhere.
SHADOW_SAMPLE_SIZE = 2000
if n_txns > SHADOW_SAMPLE_SIZE:
    recent_txns = recent_txns.sample(fraction=SHADOW_SAMPLE_SIZE / n_txns, seed=42)

recent_pdf = recent_txns.toPandas()


def _score_all(model, features, pdf):
    X = np.nan_to_num(pdf[features].values.astype(np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    scores = []
    for i in range(len(X)):
        try:
            result = model.predict(X[i:i + 1])
            scores.append(float(result["fraud_probability"]))
        except Exception:
            scores.append(-1.0)
    return scores


recent_pdf["champion_score"] = _score_all(champion_model, champion_features, recent_pdf)
recent_pdf["challenger_score"] = _score_all(challenger_model, challenger_features, recent_pdf)
recent_pdf["score_delta"] = recent_pdf["challenger_score"] - recent_pdf["champion_score"]
recent_pdf["scoring_error"] = (recent_pdf["champion_score"] < 0) | (recent_pdf["challenger_score"] < 0)
recent_pdf["champion_version"] = champion_version
recent_pdf["challenger_version"] = challenger_version

shadow_results = spark.createDataFrame(
    recent_pdf[[
        "transaction_id", "event_time_ts", "amount", "is_fraud_reconciled",
        "champion_score", "challenger_score", "score_delta",
        "scoring_error", "champion_version", "challenger_version",
    ]]
).withColumn("_shadow_scored_at", current_timestamp())


# ---------------------------------------------------------------------------
# 5. MERGE INTO — idempotent write (prevents duplicates on re-run)
# ---------------------------------------------------------------------------

shadow_results.createOrReplaceTempView("_shadow_staging")

spark.sql("""
    MERGE INTO fraud_detection_dev.gold.shadow_scoring_logs AS target
    USING _shadow_staging AS source
    ON target.transaction_id = source.transaction_id
    AND target.champion_version = source.champion_version
    AND target.challenger_version = source.challenger_version
    WHEN NOT MATCHED THEN
        INSERT *
""")


# ---------------------------------------------------------------------------
# 6. Summary statistics (exclude error rows from analytics)
# ---------------------------------------------------------------------------

valid_summary = (
    shadow_results
    .filter(col("scoring_error") == False)
    .select("score_delta")
    .toPandas()["score_delta"]
)

error_count = shadow_results.filter(col("scoring_error") == True).count()

print(
    f"Shadow Scoring Complete. "
    f"Valid: {len(valid_summary):,} | Errors: {error_count} | "
    f"Mean Δ: {valid_summary.mean():.4f} | "
    f"Std Δ: {valid_summary.std():.4f} | "
    f"Max |Δ|: {valid_summary.abs().max():.4f}"
)
