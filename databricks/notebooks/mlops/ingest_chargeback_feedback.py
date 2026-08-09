# Databricks notebook source
# MAGIC %md
# MAGIC # Chargeback & Analyst Feedback Ingestion
# MAGIC Reconciles raw transactions with delayed chargebacks and analyst manual review decisions.
# MAGIC Uses idempotent MERGE INTO for incremental updates as labels mature over time.

from pyspark.sql.functions import (
    col, when, coalesce, lit, current_timestamp, datediff, current_date
)

MATURATION_WINDOW_DAYS = 30

transactions_df = spark.table("fraud_detection_dev.silver.transactions")
analyst_decisions_df = spark.table("fraud_detection_dev.gold.analyst_decisions_sync")
chargebacks_df = spark.table("fraud_detection_dev.bronze.chargeback_reports")

reconciled_df = (
    transactions_df.alias("t")
    .join(chargebacks_df.alias("c"), col("t.transaction_id") == col("c.transaction_id"), "left")
    .join(analyst_decisions_df.alias("a"), col("t.transaction_id") == col("a.transaction_id"), "left")
    .select(
        col("t.transaction_id"),
        col("t.customer_id"),
        col("t.card_id"),
        col("t.event_time_ts"),
        col("t.amount"),
        col("t.event_date"),

        when(col("c.transaction_id").isNotNull(), lit(1))
        .when(col("a.decision_result") == "CONFIRMED_FRAUD", lit(1))
        .when(
            (col("a.decision_result") == "CONFIRMED_LEGIT") &
            (datediff(current_date(), col("t.event_date")) >= MATURATION_WINDOW_DAYS),
            lit(0)
        )
        .when(
            (col("a.decision_result").isNull()) & (col("c.transaction_id").isNull()) &
            (datediff(current_date(), col("t.event_date")) >= MATURATION_WINDOW_DAYS),
            lit(0)
        )
        .otherwise(lit(None)).alias("is_fraud_reconciled"),

        when(col("c.transaction_id").isNotNull(), lit("CHARGEBACK"))
        .when(col("a.decision_result") == "CONFIRMED_FRAUD", lit("ANALYST_CONFIRMED_FRAUD"))
        .when(col("a.decision_result") == "CONFIRMED_LEGIT", lit("ANALYST_CONFIRMED_LEGIT"))
        .when(
            datediff(current_date(), col("t.event_date")) >= MATURATION_WINDOW_DAYS,
            lit("AUTO_MATURED_LEGIT")
        )
        .otherwise(lit("MATURING")).alias("label_source"),

        when(col("c.transaction_id").isNotNull(), lit(1.0))
        .when(col("a.decision_result").isNotNull(), lit(0.9))
        .when(
            datediff(current_date(), col("t.event_date")) >= MATURATION_WINDOW_DAYS,
            lit(0.7)
        )
        .otherwise(lit(0.0)).alias("label_confidence"),

        current_timestamp().alias("_reconciled_at")
    )
)

reconciled_df.createOrReplaceTempView("_reconciled_staging")

spark.sql("""
    MERGE INTO fraud_detection_dev.gold.reconciled_labeled_transactions AS target
    USING _reconciled_staging AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED AND source.label_confidence > target.label_confidence THEN
        UPDATE SET
            target.is_fraud_reconciled = source.is_fraud_reconciled,
            target.label_source = source.label_source,
            target.label_confidence = source.label_confidence,
            target._reconciled_at = source._reconciled_at
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"Label reconciliation complete. Maturation window: {MATURATION_WINDOW_DAYS} days.")
