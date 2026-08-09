"""
Unit tests for Medallion cleaning and conforming transformations.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from fraud_detection.transformations.cleaning import (
    rename_columns_to_snake_case,
    handle_categorical_nulls,
    add_amount_features
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_cleaning").getOrCreate()


def test_rename_columns_to_snake_case(spark):
    schema = StructType([
        StructField("TransactionID", IntegerType()),
        StructField("TransactionDT", IntegerType()),
        StructField("TransactionAmt", DoubleType()),
        StructField("ProductCD", StringType()),
    ])
    data = [(1001, 86400, 150.0, "W")]
    df = spark.createDataFrame(data, schema)
    renamed = rename_columns_to_snake_case(df)

    assert "transaction_id" in renamed.columns
    assert "transaction_dt" in renamed.columns
    assert "transaction_amt" in renamed.columns
    assert "product_cd" in renamed.columns


def test_handle_categorical_nulls(spark):
    schema = StructType([
        StructField("product_cd", StringType()),
        StructField("card4", StringType()),
    ])
    data = [("W", None), (None, "VISA")]
    df = spark.createDataFrame(data, schema)
    result = handle_categorical_nulls(df, ["product_cd", "card4"])
    rows = result.collect()

    assert rows[0]["card4"] == "unknown"
    assert rows[1]["product_cd"] == "unknown"


def test_add_amount_features(spark):
    schema = StructType([
        StructField("transaction_amt", DoubleType()),
    ])
    data = [(100.0,), (50.25,)]
    df = spark.createDataFrame(data, schema)
    result = add_amount_features(df)
    rows = result.collect()

    assert rows[0]["is_round_amount"] == 1
    assert rows[1]["is_round_amount"] == 0
    assert rows[1]["amount_cents"] == 25
