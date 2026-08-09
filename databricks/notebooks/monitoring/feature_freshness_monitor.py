# Databricks notebook
# Feature Freshness & Late-Arrival Monitoring

from pyspark.sql.functions import (
    col, current_timestamp, max as smax, expr
)

# 1. Late events dropped by watermark
try:
    watermark_drops = (
        spark.table("fraud_detection_dev.quarantine.bronze_stream_parse_failures")
        .filter(col("failure_reason") == "LATE_WATERMARK_DROP")
        .filter(col("_ingested_at") >= current_timestamp() - expr("INTERVAL 24 HOURS"))
        .count()
    )
except Exception:
    watermark_drops = 0

print(f"Late events dropped by watermark (last 24h): {watermark_drops}")

# 2. Feature Materialization Lag Check
try:
    silver_max_ts = spark.table("fraud_detection_dev.silver.transactions").agg(smax("event_time_ts").alias("max_ts")).collect()[0]["max_ts"]
    print(f"Latest Silver event timestamp: {silver_max_ts}")
except Exception:
    print("Silver table empty or not available.")

print("✅ Monitoring metrics checked.")
