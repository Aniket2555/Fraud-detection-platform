# Databricks notebook
# Late-Arrival Distribution Analysis

from pyspark.sql.functions import (
    col, unix_timestamp, to_timestamp, percentile_approx,
    avg, stddev, max as smax, min as smin, count
)

bronze_df = spark.table("fraud_detection_dev.bronze.transactions")

late_arrival_df = (
    bronze_df
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .withColumn("enqueue_ts", col("_enqueued_time"))
    .withColumn("arrival_delay_seconds", unix_timestamp("enqueue_ts") - unix_timestamp("event_ts"))
    .filter(col("arrival_delay_seconds").isNotNull())
)

delay_stats = late_arrival_df.select(
    count("*").alias("total_events"),
    avg("arrival_delay_seconds").alias("mean_delay_s"),
    stddev("arrival_delay_seconds").alias("stddev_delay_s"),
    smin("arrival_delay_seconds").alias("min_delay_s"),
    smax("arrival_delay_seconds").alias("max_delay_s"),
    percentile_approx("arrival_delay_seconds", 0.50).alias("p50_delay_s"),
    percentile_approx("arrival_delay_seconds", 0.90).alias("p90_delay_s"),
    percentile_approx("arrival_delay_seconds", 0.95).alias("p95_delay_s"),
    percentile_approx("arrival_delay_seconds", 0.99).alias("p99_delay_s"),
).collect()[0]

print("=" * 60)
print("LATE-ARRIVAL DISTRIBUTION")
print("=" * 60)
print(f"Total events: {delay_stats.total_events:,}")
print(f"Mean delay:   {delay_stats.mean_delay_s:.2f}s")
print(f"P50:          {delay_stats.p50_delay_s:.2f}s")
print(f"P95:          {delay_stats.p95_delay_s:.2f}s")
print(f"P99:          {delay_stats.p99_delay_s:.2f}s")
print("=" * 60)
