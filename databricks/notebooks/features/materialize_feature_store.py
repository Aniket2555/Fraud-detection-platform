# Databricks notebook
# Materialize Feature Store Delta Tables
#
# Batch materialization (not streaming) of the velocity/geo feature
# families into Delta tables under the gold schema, so ml/training's
# point-in-time join has real feature tables to retrieve from. window()
# aggregation works the same in batch as in streaming (withWatermark is a
# no-op on a static DataFrame), so the same fraud_detection.features
# functions are reused as-is.

from pyspark.sql.functions import col
from fraud_detection.features.velocity_features import (
    compute_card_velocity_5m,
    compute_card_velocity_1h,
    compute_customer_velocity_1h,
    compute_customer_velocity_24h,
)
from fraud_detection.features.geo_features import compute_geo_velocity_batch

# customer_id/card_id/amount only exist in the Event Hub-streamed dataset,
# not the Phase 1 IEEE-CIS batch table (see Phase 3 "Known Issues").
silver_df = spark.table("fraud_detection_dev.silver.streaming_transactions")

# --- Card velocity: union the 5m and 1h window outputs on (card_id, feature_timestamp) ---
card_5m = compute_card_velocity_5m(silver_df)
card_1h = compute_card_velocity_1h(silver_df)
card_velocity = (
    card_5m.alias("a")
    .join(
        card_1h.alias("b"),
        (col("a.card_id") == col("b.card_id")) & (col("a.feature_timestamp") == col("b.feature_timestamp")),
        "outer",
    )
    .select(
        col("a.card_id").alias("card_id_a"), col("b.card_id").alias("card_id_b"),
        col("a.feature_timestamp").alias("ts_a"), col("b.feature_timestamp").alias("ts_b"),
        "vel_card_txn_count_5m", "vel_card_avg_amount_5m", "vel_card_std_amount_5m",
        "vel_card_max_amount_5m", "vel_card_sum_amount_5m",
        "vel_card_txn_count_1h", "vel_card_sum_amount_1h",
    )
    .selectExpr(
        "coalesce(card_id_a, card_id_b) as card_id",
        "coalesce(ts_a, ts_b) as feature_timestamp",
        "vel_card_txn_count_5m", "vel_card_avg_amount_5m", "vel_card_std_amount_5m",
        "vel_card_max_amount_5m", "vel_card_sum_amount_5m",
        "vel_card_txn_count_1h", "vel_card_sum_amount_1h",
    )
)
(card_velocity.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.feature_card_velocity"))

# --- Customer velocity: union the 1h and 24h window outputs ---
cust_1h = compute_customer_velocity_1h(silver_df)
cust_24h = compute_customer_velocity_24h(silver_df)
customer_velocity = (
    cust_1h.alias("a")
    .join(
        cust_24h.alias("b"),
        (col("a.customer_id") == col("b.customer_id")) & (col("a.feature_timestamp") == col("b.feature_timestamp")),
        "outer",
    )
    .selectExpr(
        "coalesce(a.customer_id, b.customer_id) as customer_id",
        "coalesce(a.feature_timestamp, b.feature_timestamp) as feature_timestamp",
        "vel_cust_txn_count_1h", "vel_cust_avg_amount_1h", "vel_cust_sum_amount_1h",
        "vel_cust_distinct_merchants_1h", "vel_cust_distinct_devices_24h",
    )
)
(customer_velocity.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.feature_customer_velocity"))

# --- Geo velocity ---
geo_df = (
    compute_geo_velocity_batch(silver_df)
    .select("card_id", "event_time_ts", "geo_dist_km", "geo_implied_speed_kmh", "geo_flag_impossible_travel")
    .withColumnRenamed("event_time_ts", "feature_timestamp")
)
(geo_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("fraud_detection_dev.gold.feature_geo_velocity"))

print(
    f"Feature store materialized: "
    f"card_velocity={card_velocity.count()}, "
    f"customer_velocity={customer_velocity.count()}, "
    f"geo_velocity={geo_df.count()}"
)
