"""
Data Preparation Pipeline: Feature Retrieval & Time-Based Splitting.
Uses Phase 3's multi_entity_pit_join for temporal correctness.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, datediff, current_date
from pyspark.sql.window import Window
from fraud_detection.features.point_in_time_join import multi_entity_pit_join

logger = logging.getLogger("data_preparation")

# customer_behavioral_baselines and merchant_risk_baselines refresh daily;
# graph_entity_metrics refreshes hourly (Phase 3 §3.5). Lookback windows are
# generous relative to that cadence so a delayed/missed run doesn't null out
# an otherwise-available feature, while still strictly forbidding any
# feature snapshot newer than the observation's event_time.
BASELINE_LOOKBACK_SECONDS = 3 * 24 * 3600   # tolerate up to 3 missed daily runs
GRAPH_LOOKBACK_SECONDS = 3 * 24 * 3600      # tolerate multiple missed hourly runs


def prepare_training_dataset(spark) -> tuple:
    """
    Retrieves labeled transactions with all feature families joined via PIT.
    Returns (train_df, val_df, test_df) as PySpark DataFrames.
    """
    # gold.reconciled_labeled_transactions (a post-hoc analyst-reviewed label
    # table) is never actually produced anywhere in this pipeline -- the
    # event schema's own is_fraud is the only label available, so it's used
    # directly here instead.
    labeled_df = (
        spark.table("fraud_detection_dev.silver.streaming_transactions")
        .withColumnRenamed("is_fraud", "is_fraud_reconciled")
        .filter(col("is_fraud_reconciled").isNotNull())
    )

    card_velocity = spark.table("fraud_detection_dev.gold.feature_card_velocity")
    cust_velocity = spark.table("fraud_detection_dev.gold.feature_customer_velocity")
    geo_velocity = spark.table("fraud_detection_dev.gold.feature_geo_velocity")
    baselines = spark.table("fraud_detection_dev.gold.customer_behavioral_baselines")
    merchant_risk = spark.table("fraud_detection_dev.gold.merchant_risk_baselines")
    # graph_entity_metrics carries all entity types in one table; pre-filter
    # to customer-typed rows and align the join key to customer_id before
    # handing it to the PIT join, same as the other customer-keyed specs.
    graph_metrics_customer = (
        spark.table("fraud_detection_dev.gold.graph_entity_metrics")
        .filter(col("entity_type") == "customer")
        .select(
            col("entity_id").alias("customer_id"),
            "graph_pagerank_score", "graph_degree_centrality", "graph_community_id",
            "feature_timestamp",
        )
    )

    # All feature families are historized snapshot tables (see Phase 3
    # "Known Issues" — compute_behavioral_baselines.py / compute_merchant_risk.py
    # / compute_graph_metrics.py append a dated snapshot per run instead of
    # overwriting in place), so every join below is genuinely point-in-time
    # safe: for a training row from months ago, this retrieves the
    # baseline/risk/graph value as it actually was at that row's event_time,
    # not today's value.
    feature_specs = [
        (card_velocity, "card_id"),
        (cust_velocity, "customer_id"),
        (geo_velocity, "card_id"),
        (baselines, "customer_id", BASELINE_LOOKBACK_SECONDS),
        (merchant_risk, "merchant_id", BASELINE_LOOKBACK_SECONDS),
        (graph_metrics_customer, "customer_id", GRAPH_LOOKBACK_SECONDS),
    ]
    enriched_df = multi_entity_pit_join(labeled_df, feature_specs)

    # event_month-based splitting assumed a multi-month dataset (e.g. IEEE-CIS's
    # 6-month span); this pipeline's actual data source is a compressed-timeframe
    # streaming replay (minutes to hours, not months), so a fixed 70/15/15
    # chronological split on event_time_ts is used instead.
    row_number_col = "_pit_split_row_number"
    ordered = enriched_df.withColumn(
        row_number_col, F.row_number().over(Window.orderBy("event_time_ts"))
    )
    total = ordered.count()
    train_end = int(total * 0.70)
    val_end = int(total * 0.85)

    train_df = ordered.filter(col(row_number_col) <= train_end).drop(row_number_col)
    val_df = ordered.filter(
        (col(row_number_col) > train_end) & (col(row_number_col) <= val_end)
    ).drop(row_number_col)
    test_df = ordered.filter(col(row_number_col) > val_end).drop(row_number_col)

    return train_df, val_df, test_df
