"""
Stateless Transaction-Level Feature Transformations.
Applied per-event without stateful windowing overhead.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, log1p, hour, dayofweek, when, lower, trim,
    sha2, concat_ws
)
from pyspark.sql.types import IntegerType

# Vocabulary must match the `payment_method`/`channel` enums in
# schemas/transaction_event_v1.json — these are the single source of
# truth for encoding, consumed by both feature index columns below.
PAYMENT_METHOD_MAP = {
    "credit_card": 1, "debit_card": 2, "prepaid": 3,
    "wallet": 4, "upi": 5, "bank_transfer": 6
}
CHANNEL_MAP = {
    "web": 1, "mobile_app": 2, "pos": 3, "atm": 4, "moto": 5, "recurring": 6
}


def _encode_column(column, value_map: dict):
    """Builds a when/otherwise chain from a lowercased/trimmed column and a value->index map."""
    normalized = lower(trim(column))
    expr = when(normalized == list(value_map.keys())[0], list(value_map.values())[0])
    for value, idx in list(value_map.items())[1:]:
        expr = expr.when(normalized == value, idx)
    return expr.otherwise(0).cast(IntegerType())


def compute_stateless_features(df: DataFrame) -> DataFrame:
    """Computes 11 stateless transformations on clean Silver transaction records."""
    return (
        df
        .withColumn("feature_log_amount", log1p(col("amount")))
        .withColumn("feature_hour_of_day", hour(col("event_time_ts")))
        .withColumn("feature_day_of_week", dayofweek(col("event_time_ts")))
        .withColumn(
            "feature_is_weekend",
            when(col("feature_day_of_week").isin([1, 7]), 1).otherwise(0)
        )
        .withColumn(
            "feature_is_night",
            when(col("feature_hour_of_day").isin([0, 1, 2, 3, 4, 5]), 1).otherwise(0)
        )
        .withColumn(
            "feature_billing_shipping_mismatch",
            when(
                col("shipping_country").isNotNull() &
                (col("billing_country") != col("shipping_country")),
                1
            ).otherwise(0)
        )
        .withColumn(
            "feature_amount_bucket",
            when(col("amount") < 10.0, "micro")
            .when((col("amount") >= 10.0) & (col("amount") < 100.0), "small")
            .when((col("amount") >= 100.0) & (col("amount") < 500.0), "medium")
            .when((col("amount") >= 500.0) & (col("amount") < 2000.0), "large")
            .otherwise("high_value")
        )
        .withColumn(
            "feature_card_merchant_pair",
            sha2(concat_ws("||", col("card_id"), col("merchant_id")), 256)
        )
        .withColumn(
            "feature_payment_method_idx",
            _encode_column(col("payment_method"), PAYMENT_METHOD_MAP)
        )
        .withColumn(
            "feature_channel_idx",
            _encode_column(col("channel"), CHANNEL_MAP)
        )
    )
