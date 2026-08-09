# Databricks notebook
# Gold Aggregation: Hourly Transaction Stats

from pyspark.sql.functions import count, sum as _sum, avg, col, when, current_timestamp

silver_df = spark.table("fraud_detection_dev.silver.transactions")

hourly_stats = (
    silver_df
        .groupBy("event_date", "event_hour")
        .agg(
            count("*").alias("txn_count"),
            _sum("transaction_amt").alias("total_amount"),
            avg("transaction_amt").alias("avg_amount"),
            _sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
            (
                _sum(when(col("is_fraud") == 1, 1).otherwise(0)) / count("*")
            ).alias("fraud_rate"),
        )
        .withColumn("_computed_at", current_timestamp())
)

(hourly_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable("fraud_detection_dev.gold.hourly_txn_stats"))

print(f"Gold hourly_txn_stats written: {spark.table('fraud_detection_dev.gold.hourly_txn_stats').count()} rows")
