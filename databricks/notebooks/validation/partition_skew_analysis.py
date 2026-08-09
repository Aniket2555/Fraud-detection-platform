# Databricks notebook
# Partition Skew Analysis

from pyspark.sql.functions import col, count, stddev, avg, max as smax, min as smin

bronze_df = spark.table("fraud_detection_dev.bronze.transactions")

partition_counts = (
    bronze_df
    .groupBy("_source_partition")
    .agg(count("*").alias("event_count"))
    .orderBy("_source_partition")
)

skew_stats = partition_counts.select(
    avg("event_count").alias("avg_per_partition"),
    stddev("event_count").alias("stddev_per_partition"),
    smax("event_count").alias("max_partition"),
    smin("event_count").alias("min_partition"),
).collect()[0]

skew_ratio = skew_stats.max_partition / max(skew_stats.min_partition, 1)

print(f"Partition skew ratio (max/min): {skew_ratio:.2f}")
print(f"  Avg per partition: {skew_stats.avg_per_partition:.0f}")
print(f"  Stddev: {skew_stats.stddev_per_partition:.0f}")
