# Databricks notebook
# Gold Aggregation: Daily Fraud Summary

from pyspark.sql.functions import (
    count, sum as _sum, avg, max as _max,
    countDistinct, col, when, current_timestamp, expr
)

silver_df = spark.table("fraud_detection_dev.silver.transactions")

daily_summary = (
    silver_df
        .groupBy("event_date")
        .agg(
            count("*").alias("total_transactions"),
            _sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_transactions"),
            _sum(when(col("is_fraud") == 0, 1).otherwise(0)).alias("legit_transactions"),
            (
                _sum(when(col("is_fraud") == 1, 1).otherwise(0)) / count("*")
            ).alias("fraud_rate"),
            _sum("transaction_amt").alias("total_amount"),
            _sum(when(col("is_fraud") == 1, col("transaction_amt"))).alias("fraud_amount"),
            _sum(when(col("is_fraud") == 0, col("transaction_amt"))).alias("legit_amount"),
            avg(when(col("is_fraud") == 1, col("transaction_amt"))).alias("avg_fraud_amount"),
            avg(when(col("is_fraud") == 0, col("transaction_amt"))).alias("avg_legit_amount"),
            expr("percentile_approx(CASE WHEN is_fraud = 1 THEN transaction_amt END, 0.5)")
                .alias("median_fraud_amount"),
            _max(when(col("is_fraud") == 1, col("transaction_amt"))).alias("max_fraud_amount"),
            countDistinct("product_cd").alias("distinct_products"),
            countDistinct("card4").alias("distinct_card_networks"),
        )
        .withColumn("_computed_at", current_timestamp())
)

(daily_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable("fraud_detection_dev.gold.daily_fraud_summary"))

print(f"Gold daily_fraud_summary written: {spark.table('fraud_detection_dev.gold.daily_fraud_summary').count()} rows")
