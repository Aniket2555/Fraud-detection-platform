"""
Unit tests for stateless features.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

from fraud_detection.features.stateless_features import compute_stateless_features


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_stateless").getOrCreate()


def test_compute_stateless_features(spark):
    schema = StructType([
        StructField("amount", DoubleType()),
        StructField("event_time_ts", TimestampType()),
        StructField("shipping_country", StringType()),
        StructField("billing_country", StringType()),
        StructField("card_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("payment_method", StringType()),
        StructField("channel", StringType()),
    ])
    now = datetime(2025, 1, 1, 14, 0, 0)
    data = [(100.0, now, "US", "IN", "card_1", "merch_1", "credit_card", "mobile_app")]
    df = spark.createDataFrame(data, schema)
    result = compute_stateless_features(df)
    row = result.collect()[0]

    assert row["feature_is_weekend"] == 0
    assert row["feature_is_night"] == 0
    assert row["feature_billing_shipping_mismatch"] == 1
    assert row["feature_payment_method_idx"] == 1
    assert row["feature_channel_idx"] == 2
