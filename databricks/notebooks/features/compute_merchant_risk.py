# Databricks notebook
# Merchant Risk Baseline Features (30-Day Rolling Window)
#
# Historized snapshot table (fix applied — see Phase 3 "Known Issues"):
# see compute_behavioral_baselines.py for the rationale. Each day's computed
# risk ratio is appended as its own row keyed on (merchant_id, snapshot_date)
# instead of overwriting the merchant's single row in place, so training
# rows can PIT-join against the risk value that was current at event_time.

from pyspark.sql.functions import (
    col, avg, count, sum as ssum, when, current_timestamp,
    current_date, datediff, coalesce, lit
)
from delta.tables import DeltaTable

# See compute_behavioral_baselines.py -- merchant_id/amount only exist in
# the Event Hub-streamed dataset, not the Phase 1 IEEE-CIS batch table.
silver_df = spark.table("fraud_detection_dev.silver.streaming_transactions")

merchant_df = (
    silver_df
    .filter(datediff(current_date(), col("event_time_ts")) <= 30)
    .groupBy("merchant_id")
    .agg(
        avg("amount").alias("merch_avg_ticket_30d"),
        count("transaction_id").alias("merch_txn_count_30d"),
        ssum(when(col("is_fraud") == True, 1).otherwise(0)).alias("merch_fraud_count_30d"),
        count("transaction_id").alias("merch_total_count_30d")
    )
    .withColumn(
        "merch_fraud_ratio_30d",
        when(col("merch_total_count_30d") > 0,
             col("merch_fraud_count_30d") / col("merch_total_count_30d"))
        .otherwise(lit(0.0))
    )
    .drop("merch_total_count_30d")
    .withColumn("snapshot_date", current_date())
    .withColumn("feature_timestamp", current_timestamp())
)

target_table_name = "fraud_detection_dev.gold.merchant_risk_baselines"

if spark.catalog.tableExists(target_table_name):
    target = DeltaTable.forName(spark, target_table_name)
    (
        target.alias("t")
        .merge(merchant_df.alias("s"), "t.merchant_id = s.merchant_id AND t.snapshot_date = s.snapshot_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        merchant_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .saveAsTable(target_table_name)
    )

print(f"✅ Merchant risk features materialized: {merchant_df.count()} merchants updated for snapshot_date.")
