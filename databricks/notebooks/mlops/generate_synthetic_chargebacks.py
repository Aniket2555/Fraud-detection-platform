# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Chargeback Report Generator (dev/test only)
# MAGIC There is no real payment processor or card scheme feeding chargeback reports into
# MAGIC this Free-Trial dev environment. `bronze.chargeback_reports` is a genuine input this
# MAGIC pipeline depends on (real chargebacks are the 100%-ground-truth label source per the
# MAGIC Phase 6 plan), so this notebook seeds it with clearly-synthetic rows to exercise the
# MAGIC reconciliation logic end-to-end.
# MAGIC
# MAGIC In production this table would be populated by an actual chargeback/scheme-report
# MAGIC ingestion feed -- this notebook is a stand-in for that feed, not a replacement for it.

from pyspark.sql.functions import lit, current_timestamp

spark.sql("""
    CREATE TABLE IF NOT EXISTS fraud_detection_dev.bronze.chargeback_reports (
        transaction_id STRING,
        chargeback_reason STRING,
        chargeback_amount DOUBLE,
        reported_at TIMESTAMP,
        _is_synthetic BOOLEAN,
        _ingested_at TIMESTAMP
    ) USING DELTA
""")

# One row deliberately chosen to test the CHARGEBACK > ANALYST_CONFIRMED_LEGIT priority
# rule (checklist item #2): this transaction_id already has a synthetic
# ANALYST_CONFIRMED_LEGIT decision in Azure SQL (inserted via sp_upsert_fraud_case +
# a manual analyst_decisions row) -- the chargeback below should override it.
synthetic_chargebacks = [
    ("b36fc970-381a-44db-94d6-f6cd6461fe9d", "FRAUDULENT_TRANSACTION_UNAUTHORIZED", 250.0, True),
]

df = spark.createDataFrame(
    synthetic_chargebacks,
    ["transaction_id", "chargeback_reason", "chargeback_amount", "_is_synthetic"]
).withColumn("reported_at", current_timestamp()).withColumn("_ingested_at", current_timestamp())

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.bronze.chargeback_reports")
)

print(f"Seeded {df.count()} synthetic chargeback report(s).")
