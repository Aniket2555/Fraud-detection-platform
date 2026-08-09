"""
Unit tests for the point-in-time feature join engine.

Phase 3's own plan calls this "the single most important validation in the
entire pipeline" -- if it's wrong, models silently train on future
information and offline metrics collapse in production. These tests were
missing entirely; added alongside the fix that historizes
customer_behavioral_baselines / merchant_risk_baselines / graph_entity_metrics
so they can be safely PIT-joined instead of latest-known-value joined.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

from fraud_detection.features.point_in_time_join import (
    point_in_time_feature_join,
    multi_entity_pit_join,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_pit_join").getOrCreate()


def _obs_df(spark, rows):
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("card_id", StringType()),
        StructField("event_time_ts", TimestampType()),
    ])
    return spark.createDataFrame(rows, schema)


def _feat_df(spark, rows):
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("feature_timestamp", TimestampType()),
        StructField("feature_value", DoubleType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_picks_latest_feature_not_after_observation(spark):
    """Of several eligible snapshots, the most recent one <= event_time wins."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    obs = _obs_df(spark, [("t1", "card_1", now)])
    feat = _feat_df(spark, [
        ("card_1", now - timedelta(days=2), 10.0),
        ("card_1", now - timedelta(days=1), 20.0),   # latest eligible snapshot
        ("card_1", now - timedelta(minutes=1), 30.0),  # closer, but still eligible
    ])
    result = point_in_time_feature_join(
        obs, feat, entity_key="card_id", max_lookback_seconds=7 * 86400
    ).collect()
    assert len(result) == 1
    assert result[0]["feature_value"] == 30.0


def test_no_future_leakage(spark):
    """A feature snapshot computed AFTER the observation must never be joined,
    even when it's the only snapshot that exists."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    obs = _obs_df(spark, [("t1", "card_1", now)])
    feat = _feat_df(spark, [
        ("card_1", now + timedelta(minutes=5), 999.0),  # from the future
    ])
    result = point_in_time_feature_join(
        obs, feat, entity_key="card_id", max_lookback_seconds=7 * 86400
    ).collect()
    assert len(result) == 1
    assert result[0]["feature_value"] is None


def test_new_entity_returns_null_not_error(spark):
    """An entity with no feature history at all resolves to NULL, not a crash."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    obs = _obs_df(spark, [("t1", "card_never_seen", now)])
    feat = _feat_df(spark, [
        ("card_1", now - timedelta(days=1), 20.0),
    ])
    result = point_in_time_feature_join(
        obs, feat, entity_key="card_id", max_lookback_seconds=7 * 86400
    ).collect()
    assert len(result) == 1
    assert result[0]["feature_value"] is None


def test_lookback_window_excludes_stale_snapshot(spark):
    """A snapshot older than max_lookback_seconds is excluded (returns NULL)
    instead of silently attaching a stale, out-of-window value."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    obs = _obs_df(spark, [("t1", "card_1", now)])
    feat = _feat_df(spark, [
        ("card_1", now - timedelta(days=10), 5.0),
    ])
    result = point_in_time_feature_join(
        obs, feat, entity_key="card_id", max_lookback_seconds=3 * 86400
    ).collect()
    assert len(result) == 1
    assert result[0]["feature_value"] is None


def _feat_df_named(spark, rows, value_col: str):
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("feature_timestamp", TimestampType()),
        StructField(value_col, DoubleType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_multi_entity_pit_join_supports_per_spec_lookback(spark):
    """multi_entity_pit_join accepts a mix of 2-tuple (default lookback) and
    3-tuple (explicit lookback) specs, chaining both correctly -- mirroring
    ml/training/data_preparation.py, where fast-refreshing velocity features
    (default lookback) and slow-refreshing baseline/graph features (longer,
    explicit lookback) are joined in the same call."""
    now = datetime(2026, 1, 15, 12, 0, 0)
    obs = _obs_df(spark, [("t1", "card_1", now)])

    fast_feat = _feat_df_named(spark, [("card_1", now - timedelta(hours=1), 1.0)], "vel_value")
    # Only eligible under a lookback > 1 day; default (86400s) would exclude it.
    slow_feat = _feat_df_named(spark, [("card_1", now - timedelta(days=2), 2.0)], "base_value")

    result = multi_entity_pit_join(
        obs,
        [
            (fast_feat, "card_id"),                       # default 86400s lookback
            (slow_feat, "card_id", 3 * 86400),             # explicit longer lookback
        ],
    ).collect()

    assert len(result) == 1
    assert result[0]["vel_value"] == 1.0
    assert result[0]["base_value"] == 2.0
