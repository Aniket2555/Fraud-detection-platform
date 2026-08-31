# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Decision Audit Log (Phase 5 -> Phase 6 bridge)
# MAGIC Phase 5's `audit_logger` Azure Function writes one JSON file per decision event to
# MAGIC ADLS Gen2 (`gold/audit_logs/year=YYYY/month=MM/day=DD/<txn>_<msg>.json`) -- there was
# MAGIC never a queryable Delta table version of this data, which `track_model_performance_kpis.py`,
# MAGIC `run_daily_drift_check.py`, and `automated_rollback_sentinel.py` all require
# MAGIC (`gold.decision_audit_log`). This notebook reads the raw JSON files and materializes
# MAGIC that table, re-runnable incrementally via Auto Loader-style file tracking would be the
# MAGIC production version; this dev version does a full re-read + overwrite since audit
# MAGIC volume here is small.
# MAGIC
# MAGIC Column mapping: the JSON's `fraud_score` field is renamed to `fraud_probability` to
# MAGIC match what every downstream Phase 6 consumer expects.

from pyspark.sql.functions import col, to_timestamp

raw_df = spark.read.json("abfss://gold@stfraudlakedev.dfs.core.windows.net/audit_logs/")

decision_audit_df = (
    raw_df
    .select(
        col("transaction_id"),
        col("fraud_score").cast("double").alias("fraud_probability"),
        to_timestamp(col("audit_timestamp_utc")).alias("audit_timestamp_utc"),
        col("decision_action"),
        col("scoring_mode"),
        col("model_version"),
        col("amount").cast("double").alias("amount"),
        col("currency"),
        col("customer_id"),
        col("card_id"),
        col("correlation_id"),
    )
    .filter(col("transaction_id").isNotNull())
)

(
    decision_audit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.decision_audit_log")
)

count = decision_audit_df.count()
print(f"Ingested {count} decision audit record(s) into gold.decision_audit_log.")
