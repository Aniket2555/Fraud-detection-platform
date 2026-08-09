# Databricks notebook
# Customer Behavioral Baseline (90-Day Rolling Window)
#
# Historized snapshot table (fix applied — see Phase 3 "Known Issues"):
# previously this MERGE-upserted a single row per customer_id, overwriting
# each day's value in place. That made a correct point-in-time join
# impossible — any training row, however old, would join against *today's*
# baseline. Now each day's computed value is appended as its own row keyed
# on (customer_id, snapshot_date), partitioned by snapshot_date, so
# ml/training/data_preparation.py can PIT-join against the baseline that
# was actually current as of each transaction's event_time.

from pyspark.sql.functions import (
    col, avg, stddev, count, min as smin, max as smax, current_timestamp,
    current_date, datediff, coalesce, lit, percentile_approx
)
from delta.tables import DeltaTable

silver_df = spark.table("fraud_detection_dev.silver.transactions")

baseline_df = (
    silver_df
    .filter(datediff(current_date(), col("event_date")) <= 90)
    .groupBy("customer_id")
    .agg(
        avg("amount").alias("base_cust_90d_avg_amount"),
        coalesce(stddev("amount"), lit(0.0)).alias("base_cust_90d_std_amount"),
        count("transaction_id").alias("base_cust_90d_txn_count"),
        percentile_approx("amount", 0.5).alias("base_cust_90d_median_amount"),
        smin("event_time_ts").alias("base_cust_first_seen_ts"),
        smax("event_time_ts").alias("base_cust_last_seen_ts")
    )
    .withColumn("base_cust_tenure_days", datediff(current_date(), col("base_cust_first_seen_ts")))
    .withColumn("snapshot_date", current_date())
    .withColumn("feature_timestamp", current_timestamp())
)

target_table_name = "fraud_detection_dev.gold.customer_behavioral_baselines"

if spark.catalog.tableExists(target_table_name):
    target = DeltaTable.forName(spark, target_table_name)
    (
        target.alias("t")
        # Keyed on (customer_id, snapshot_date): a same-day rerun updates
        # that day's row in place (idempotent), but different days
        # accumulate as separate historized rows instead of overwriting.
        .merge(baseline_df.alias("s"), "t.customer_id = s.customer_id AND t.snapshot_date = s.snapshot_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        baseline_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("snapshot_date")
        .saveAsTable(target_table_name)
    )

print(f"✅ Baseline features materialized: {baseline_df.count()} customers updated for snapshot_date.")
