# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Analyst Decisions from Azure SQL Case Management DB
# MAGIC Reads `fraud_cases` JOIN `analyst_decisions` from the Phase 5 Azure SQL database
# MAGIC via JDBC and writes a Delta snapshot table keyed by transaction_id, matching the
# MAGIC shape `ingest_chargeback_feedback.py` expects (`gold.analyst_decisions_sync`).
# MAGIC
# MAGIC `analyst_decisions` itself is keyed by `case_id`, not `transaction_id` (see
# MAGIC `database/migrations/V003__create_analyst_decisions.sql`), so the join back to
# MAGIC `fraud_cases` is required to project `transaction_id`.

from pyspark.sql.functions import col

jdbc_url = dbutils.secrets.get(scope="kv-fraud", key="azure-sql-jdbc-url")

query = """
(
    SELECT
        fc.transaction_id,
        ad.analyst_id,
        ad.decision_result,
        ad.decision_notes,
        ad.decided_at
    FROM analyst_decisions ad
    JOIN fraud_cases fc ON fc.case_id = ad.case_id
) AS analyst_decisions_joined
"""

analyst_decisions_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .load()
)

(
    analyst_decisions_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.analyst_decisions_sync")
)

count = analyst_decisions_df.count()
print(f"Synced {count} analyst decision(s) to gold.analyst_decisions_sync.")
