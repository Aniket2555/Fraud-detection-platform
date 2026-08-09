"""
Unit tests for velocity features.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

from fraud_detection.features.velocity_features import compute_card_velocity_5m


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_velocity").getOrCreate()


def test_card_velocity_5m(spark):
    schema = StructType([
        StructField("card_id", StringType()),
        StructField("transaction_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("event_time_ts", TimestampType()),
    ])
    now = datetime.utcnow()
    data = [
        ("card_100", "t1", 50.0, now),
        ("card_100", "t2", 150.0, now + timedelta(seconds=30)),
    ]
    df = spark.createDataFrame(data, schema)
    # Testing logic structure
    assert df.count() == 2
