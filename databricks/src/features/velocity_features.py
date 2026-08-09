"""
Stateful Velocity Feature Computation using Spark Structured Streaming.
Computes multi-window aggregations for card_id and customer_id.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, avg, stddev, max as smax, sum as ssum,
    coalesce, lit, approx_count_distinct, current_timestamp
)

WATERMARK_DELAY = "10 minutes"


def compute_card_velocity_5m(silver_stream: DataFrame) -> DataFrame:
    """Computes 5-minute sliding velocity features per card_id."""
    return (
        silver_stream
        .withWatermark("event_time_ts", WATERMARK_DELAY)
        .groupBy(
            col("card_id"),
            window(col("event_time_ts"), "5 minutes", "1 minute")
        )
        .agg(
            count("transaction_id").alias("vel_card_txn_count_5m"),
            avg("amount").alias("vel_card_avg_amount_5m"),
            coalesce(stddev("amount"), lit(0.0)).alias("vel_card_std_amount_5m"),
            smax("amount").alias("vel_card_max_amount_5m"),
            ssum("amount").alias("vel_card_sum_amount_5m")
        )
        .select(
            col("card_id"),
            col("window.end").alias("feature_timestamp"),
            col("vel_card_txn_count_5m"),
            col("vel_card_avg_amount_5m"),
            col("vel_card_std_amount_5m"),
            col("vel_card_max_amount_5m"),
            col("vel_card_sum_amount_5m"),
            current_timestamp().alias("_materialized_at")
        )
    )


def compute_card_velocity_1h(silver_stream: DataFrame) -> DataFrame:
    """Computes 1-hour sliding velocity features per card_id."""
    return (
        silver_stream
        .withWatermark("event_time_ts", WATERMARK_DELAY)
        .groupBy(
            col("card_id"),
            window(col("event_time_ts"), "1 hour", "5 minutes")
        )
        .agg(
            count("transaction_id").alias("vel_card_txn_count_1h"),
            ssum("amount").alias("vel_card_sum_amount_1h")
        )
        .select(
            col("card_id"),
            col("window.end").alias("feature_timestamp"),
            col("vel_card_txn_count_1h"),
            col("vel_card_sum_amount_1h"),
            current_timestamp().alias("_materialized_at")
        )
    )


def compute_customer_velocity_1h(silver_stream: DataFrame) -> DataFrame:
    """Computes 1-hour sliding velocity features per customer_id."""
    return (
        silver_stream
        .withWatermark("event_time_ts", WATERMARK_DELAY)
        .groupBy(
            col("customer_id"),
            window(col("event_time_ts"), "1 hour", "5 minutes")
        )
        .agg(
            count("transaction_id").alias("vel_cust_txn_count_1h"),
            avg("amount").alias("vel_cust_avg_amount_1h"),
            ssum("amount").alias("vel_cust_sum_amount_1h"),
            approx_count_distinct("merchant_id").alias("vel_cust_distinct_merchants_1h")
        )
        .select(
            col("customer_id"),
            col("window.end").alias("feature_timestamp"),
            col("vel_cust_txn_count_1h"),
            col("vel_cust_avg_amount_1h"),
            col("vel_cust_sum_amount_1h"),
            col("vel_cust_distinct_merchants_1h"),
            current_timestamp().alias("_materialized_at")
        )
    )


def compute_customer_velocity_24h(silver_stream: DataFrame) -> DataFrame:
    """Computes 24-hour window features per customer_id."""
    return (
        silver_stream
        .withWatermark("event_time_ts", WATERMARK_DELAY)
        .groupBy(
            col("customer_id"),
            window(col("event_time_ts"), "24 hours")
        )
        .agg(
            approx_count_distinct("device_id").alias("vel_cust_distinct_devices_24h")
        )
        .select(
            col("customer_id"),
            col("window.end").alias("feature_timestamp"),
            col("vel_cust_distinct_devices_24h"),
            current_timestamp().alias("_materialized_at")
        )
    )
