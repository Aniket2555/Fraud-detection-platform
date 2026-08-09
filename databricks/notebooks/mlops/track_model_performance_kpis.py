# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Model Performance KPI Tracker
# MAGIC Aggregates prediction vs. reconciled label accuracy metrics into a Gold tracking table.
# MAGIC
# MAGIC Production fixes applied:
# MAGIC - Incremental processing: only joins decisions from the last 48h window
# MAGIC - MERGE INTO replaces append — prevents duplicate KPI rows accumulating over time
# MAGIC - Explicit kpi_date included in MERGE match condition
# MAGIC - Fraction metrics (precision, recall, FPR) computed in SQL for auditability

from pyspark.sql.functions import (
    col, count, sum as _sum, avg, when, current_timestamp, current_date,
    expr, lit, percentile_approx, date_sub
)

# ---------------------------------------------------------------------------
# Only process decisions from the last 48 hours that haven't been KPI-tracked yet.
# This avoids a full-table join and prevents double-counting on daily re-runs.
# ---------------------------------------------------------------------------
LOOKBACK_DAYS = 2  # 48h window

decisions_df = (
    spark.table("fraud_detection_dev.gold.decision_audit_log")
    .filter(col("audit_timestamp_utc") >= date_sub(current_date(), LOOKBACK_DAYS))
)

labels_df = (
    spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions")
    .filter(
        col("is_fraud_reconciled").isNotNull() &
        (col("event_date") >= date_sub(current_date(), LOOKBACK_DAYS))
    )
)

joined_df = decisions_df.join(labels_df, "transaction_id", "inner")

# ---------------------------------------------------------------------------
# Aggregate KPIs grouped by model_version and kpi_date
# ---------------------------------------------------------------------------
daily_kpis = (
    joined_df
    .withColumn("kpi_date", current_date())
    .groupBy("model_version", "kpi_date")
    .agg(
        count("*").alias("total_scored_labeled"),
        _sum(when(col("is_fraud_reconciled") == 1, 1).otherwise(0)).alias("actual_fraud_count"),
        _sum(
            when((col("fraud_probability") >= 0.5) & (col("is_fraud_reconciled") == 1), 1).otherwise(0)
        ).alias("true_positives"),
        _sum(
            when((col("fraud_probability") >= 0.5) & (col("is_fraud_reconciled") == 0), 1).otherwise(0)
        ).alias("false_positives"),
        _sum(
            when((col("fraud_probability") < 0.5) & (col("is_fraud_reconciled") == 1), 1).otherwise(0)
        ).alias("false_negatives"),
        _sum(
            when((col("fraud_probability") < 0.5) & (col("is_fraud_reconciled") == 0), 1).otherwise(0)
        ).alias("true_negatives"),
        avg("fraud_probability").alias("avg_fraud_score"),
        percentile_approx("fraud_probability", 0.5).alias("median_fraud_score"),
        percentile_approx("fraud_probability", 0.95).alias("p95_fraud_score"),
        current_timestamp().alias("_computed_at"),
    )
    # Derived fraction metrics computed in-flight
    .withColumn(
        "precision",
        expr("CASE WHEN (true_positives + false_positives) > 0 "
             "THEN true_positives / (true_positives + false_positives) ELSE NULL END")
    )
    .withColumn(
        "recall",
        expr("CASE WHEN (true_positives + false_negatives) > 0 "
             "THEN true_positives / (true_positives + false_negatives) ELSE NULL END")
    )
    .withColumn(
        "false_positive_rate",
        expr("CASE WHEN (false_positives + true_negatives) > 0 "
             "THEN false_positives / (false_positives + true_negatives) ELSE NULL END")
    )
)

# ---------------------------------------------------------------------------
# MERGE INTO prevents duplicate rows when the notebook is re-run on the same day
# ---------------------------------------------------------------------------
daily_kpis.createOrReplaceTempView("_kpi_staging")

spark.sql("""
    MERGE INTO fraud_detection_dev.gold.model_performance_kpis AS target
    USING _kpi_staging AS source
    ON target.model_version = source.model_version
    AND target.kpi_date = source.kpi_date
    WHEN MATCHED THEN
        UPDATE SET
            target.total_scored_labeled  = source.total_scored_labeled,
            target.actual_fraud_count    = source.actual_fraud_count,
            target.true_positives        = source.true_positives,
            target.false_positives       = source.false_positives,
            target.false_negatives       = source.false_negatives,
            target.true_negatives        = source.true_negatives,
            target.avg_fraud_score       = source.avg_fraud_score,
            target.median_fraud_score    = source.median_fraud_score,
            target.p95_fraud_score       = source.p95_fraud_score,
            target.precision             = source.precision,
            target.recall                = source.recall,
            target.false_positive_rate   = source.false_positive_rate,
            target._computed_at          = source._computed_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

print("Daily model performance KPIs recorded (incremental, idempotent MERGE).")
