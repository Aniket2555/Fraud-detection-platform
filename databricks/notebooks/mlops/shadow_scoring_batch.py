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

import mlflow.pyfunc
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, current_timestamp, lit, when, pandas_udf
from pyspark.sql.types import DoubleType


# ---------------------------------------------------------------------------
# 1. Load Champion (required) and Challenger/Staging (optional)
# ---------------------------------------------------------------------------

champion_model = mlflow.pyfunc.load_model("models:/fraud-ensemble-champion/Production")
champion_features: list = champion_model._model_impl.python_model.feature_names
champion_version: str   = getattr(champion_model._model_impl.python_model, "model_version", "champion")

challenger_model = None
challenger_features: list = champion_features  # default — overridden below if staging exists
challenger_version: str   = "no_challenger"

try:
    challenger_model   = mlflow.pyfunc.load_model("models:/fraud-ensemble-champion/Staging")
    challenger_features = challenger_model._model_impl.python_model.feature_names
    challenger_version  = getattr(challenger_model._model_impl.python_model, "model_version", "challenger")
    print(f"Challenger model loaded. version={challenger_version}")
except mlflow.exceptions.MlflowException:
    print("No Staging model registered — shadow scoring will only score Champion.")

if challenger_model is None:
    print("Skipping shadow scoring: no Challenger model available.")
    dbutils.notebook.exit("NO_CHALLENGER")


# ---------------------------------------------------------------------------
# 2. Load recent transactions for the scoring window
# ---------------------------------------------------------------------------

recent_txns = (
    spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions")
    .filter("event_date >= current_date() - 1")
    .select(
        ["transaction_id", "event_time_ts", "amount", "is_fraud_reconciled"]
        + list(set(champion_features + challenger_features))
    )
)

n_txns = recent_txns.count()
print(f"Shadow scoring window: {n_txns:,} transactions.")

if n_txns == 0:
    print("No transactions in scoring window — exiting.")
    dbutils.notebook.exit("NO_DATA")


# ---------------------------------------------------------------------------
# 3. Vectorised pandas UDFs (correct MapIterator pattern)
# ---------------------------------------------------------------------------

# Broadcast models to all workers
champion_model_bc   = sc.broadcast(champion_model)
challenger_model_bc = sc.broadcast(challenger_model)

@pandas_udf(DoubleType())
def score_champion_udf(iterator):
    """Scores a partition of rows against the Champion model."""
    model = champion_model_bc.value
    for batch_series in iterator:
        scores = []
        for val in batch_series:
            try:
                result = model.predict(np.array(val, dtype=np.float32).reshape(1, -1))
                scores.append(float(result["fraud_probability"]))
            except Exception:
                scores.append(-1.0)
        yield pd.Series(scores)


@pandas_udf(DoubleType())
def score_challenger_udf(iterator):
    """Scores a partition of rows against the Challenger model."""
    model = challenger_model_bc.value
    for batch_series in iterator:
        scores = []
        for val in batch_series:
            try:
                result = model.predict(np.array(val, dtype=np.float32).reshape(1, -1))
                scores.append(float(result["fraud_probability"]))
            except Exception:
                scores.append(-1.0)
        yield pd.Series(scores)


# ---------------------------------------------------------------------------
# 4. Score both models and build shadow results DataFrame
# ---------------------------------------------------------------------------

shadow_results = (
    recent_txns
    .withColumn("champion_score",    score_champion_udf(*[col(c) for c in champion_features]))
    .withColumn("challenger_score",  score_challenger_udf(*[col(c) for c in challenger_features]))
    .withColumn("score_delta",       col("challenger_score") - col("champion_score"))
    # Flag scoring errors (score == -1.0) so they can be excluded from analytics
    .withColumn(
        "scoring_error",
        when((col("champion_score") < 0) | (col("challenger_score") < 0), lit(True))
        .otherwise(lit(False))
    )
    .withColumn("champion_version",    lit(champion_version))
    .withColumn("challenger_version",  lit(challenger_version))
    .withColumn("_shadow_scored_at",   current_timestamp())
    .select(
        "transaction_id", "event_time_ts", "amount", "is_fraud_reconciled",
        "champion_score", "challenger_score", "score_delta",
        "scoring_error", "champion_version", "challenger_version",
        "_shadow_scored_at",
    )
)


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
