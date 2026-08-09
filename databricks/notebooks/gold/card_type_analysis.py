# Databricks notebook
# Gold Aggregation: Card Type Analysis

from pyspark.sql.functions import count, sum as _sum, avg, col, when, current_timestamp

silver_df = spark.table("fraud_detection_dev.silver.transactions")

card_analysis = (
    silver_df
        .groupBy("card4", "card6")
        .agg(
            count("*").alias("total_transactions"),
            _sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
            (
                _sum(when(col("is_fraud") == 1, 1).otherwise(0)) / count("*")
            ).alias("fraud_rate"),
            avg("transaction_amt").alias("avg_amount"),
            avg(when(col("is_fraud") == 1, col("transaction_amt"))).alias("avg_fraud_amount"),
        )
        .withColumn("_computed_at", current_timestamp())
)

(card_analysis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.card_type_analysis"))

print(f"Gold card_type_analysis written: {spark.table('fraud_detection_dev.gold.card_type_analysis').count()} rows")
