"""
Unit Tests for Geo-Velocity Features.
Validates Haversine distance accuracy and impossible travel flag logic.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

from fraud_detection.features.geo_features import compute_geo_velocity_batch


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_geo_features").getOrCreate()


def test_haversine_new_york_to_london(spark):
    """Known distance: New York (40.7128, -74.0060) → London (51.5074, -0.1278) ≈ 5,570 km."""
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("event_time_ts", TimestampType()),
        StructField("transaction_id", StringType()),
    ])
    now = datetime.utcnow()
    data = [
        ("card_001", 40.7128, -74.0060, now - timedelta(hours=2), "txn_1"),
        ("card_001", 51.5074, -0.1278, now, "txn_2"),
    ]
    df = spark.createDataFrame(data, schema)
    result = compute_geo_velocity_batch(df)
    london_row = result.filter(col("transaction_id") == "txn_2").collect()[0]

    assert 5500 < london_row["geo_dist_km"] < 5700
    assert london_row["geo_implied_speed_kmh"] > 2500
    assert london_row["geo_flag_impossible_travel"] == 1


def test_same_location_no_travel(spark):
    """Consecutive transactions from same coordinates → distance = 0, speed = 0."""
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("event_time_ts", TimestampType()),
        StructField("transaction_id", StringType()),
    ])
    now = datetime.utcnow()
    data = [
        ("card_002", 28.6139, 77.2090, now - timedelta(minutes=10), "txn_3"),
        ("card_002", 28.6139, 77.2090, now, "txn_4"),
    ]
    df = spark.createDataFrame(data, schema)
    result = compute_geo_velocity_batch(df)
    row = result.filter(col("transaction_id") == "txn_4").collect()[0]

    assert abs(row["geo_dist_km"]) < 0.01
    assert row["geo_flag_impossible_travel"] == 0


def test_first_transaction_defaults_to_zero(spark):
    """First transaction for a card has no predecessor → all geo features default to 0."""
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("event_time_ts", TimestampType()),
        StructField("transaction_id", StringType()),
    ])
    data = [("card_003", 19.0760, 72.8777, datetime.utcnow(), "txn_5")]
    df = spark.createDataFrame(data, schema)
    result = compute_geo_velocity_batch(df)
    row = result.collect()[0]

    assert row["geo_dist_km"] == 0.0
    assert row["geo_flag_impossible_travel"] == 0
