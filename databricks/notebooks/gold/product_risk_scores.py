# Databricks notebook
# Gold Aggregation: Product Risk Scores

from pyspark.sql.functions import count, sum as _sum, avg, col, when, lit, current_timestamp

silver_df = spark.table("fraud_detection_dev.silver.transactions")

product_risk = (
    silver_df
        .groupBy("product_cd")
        .agg(
            count("*").alias("total_transactions"),
            _sum(when(col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
            (
                _sum(when(col("is_fraud") == 1, 1).otherwise(0)) / count("*")
            ).alias("fraud_rate"),
            avg("transaction_amt").alias("avg_ticket_size"),
            _sum("transaction_amt").alias("total_volume"),
        )
        .withColumn("risk_tier",
            when(col("fraud_rate") > 0.05, lit("high"))
            .when(col("fraud_rate") > 0.02, lit("medium"))
            .otherwise(lit("low"))
        )
        .withColumn("_computed_at", current_timestamp())
)

(product_risk.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.product_risk_scores"))

print(f"Gold product_risk_scores written: {spark.table('fraud_detection_dev.gold.product_risk_scores').count()} rows")
