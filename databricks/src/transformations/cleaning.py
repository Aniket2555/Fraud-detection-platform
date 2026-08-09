"""
Shared cleaning functions for Silver layer.
Used by both batch (Phase 1) and streaming (Phase 2+) pipelines.

Production principle: Every function is stateless, deterministic, and testable
in isolation without Spark (pure column transformations).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lower, trim, regexp_replace, log1p, lit,
    split, element_at, coalesce, to_timestamp, from_unixtime,
    unix_timestamp, concat, floor
)
from pyspark.sql.types import StringType, IntegerType, DoubleType


REFERENCE_TIMESTAMP = "2025-01-01T00:00:00Z"
UNKNOWN_SENTINEL = "unknown"
RARE_DOMAIN_SENTINEL = "rare_domain"
RARE_DEVICE_SENTINEL = "other_device"


def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    """Rename all columns from PascalCase/camelCase to snake_case."""
    import re
    renamed = df
    for col_name in df.columns:
        snake = col_name
        if col_name == "TransactionID":
            snake = "transaction_id"
        elif col_name == "TransactionDT":
            snake = "transaction_dt"
        elif col_name == "TransactionAmt":
            snake = "transaction_amt"
        elif col_name == "isFraud":
            snake = "is_fraud"
        elif col_name == "ProductCD":
            snake = "product_cd"
        elif col_name == "DeviceType":
            snake = "device_type"
        elif col_name == "DeviceInfo":
            snake = "device_info"
        elif col_name.startswith("_"):
            continue
        else:
            snake = re.sub(r'(?<!^)(?=[A-Z])', '_', col_name).lower()
            if col_name.startswith("P_") or col_name.startswith("R_"):
                snake = col_name.lower()

        if snake != col_name:
            renamed = renamed.withColumnRenamed(col_name, snake)
    return renamed


def synthesize_timestamps(df: DataFrame) -> DataFrame:
    """
    Convert TransactionDT (seconds from reference) to proper timestamps.
    Anchor to a fixed reference date for reproducibility.
    """
    return (df
        .withColumn("event_time",
            to_timestamp(
                from_unixtime(
                    unix_timestamp(lit(REFERENCE_TIMESTAMP)) + col("transaction_dt")
                )
            ))
        .withColumn("event_date", col("event_time").cast("date"))
        .withColumn("event_hour", (col("transaction_dt") % 86400 / 3600).cast("int"))
        .withColumn("event_day_of_week", (floor(col("transaction_dt") / 86400) % 7).cast("int"))
    )


def handle_categorical_nulls(df: DataFrame, columns: list) -> DataFrame:
    """Replace null categoricals with 'unknown' sentinel."""
    result = df
    for c in columns:
        if c in df.columns:
            result = result.withColumn(c, coalesce(lower(trim(col(c))), lit(UNKNOWN_SENTINEL)))
    return result


def handle_numeric_null_flags(df: DataFrame, columns: list) -> DataFrame:
    """Add _is_null indicator flags for numeric columns."""
    result = df
    for c in columns:
        if c in df.columns:
            result = result.withColumn(f"{c}_is_null", when(col(c).isNull(), lit(1)).otherwise(lit(0)))
    return result


def normalize_email_domains(df: DataFrame, column: str) -> DataFrame:
    """Normalize email domains into provider and TLD."""
    if column not in df.columns:
        return df

    provider_col = f"{column}_provider"
    tld_col = f"{column}_tld"

    return (df
        .withColumn(column, lower(trim(col(column))))
        .withColumn(provider_col,
            coalesce(element_at(split(col(column), "\\."), 1), lit(UNKNOWN_SENTINEL)))
        .withColumn(tld_col,
            coalesce(element_at(split(col(column), "\\."), -1), lit(UNKNOWN_SENTINEL)))
    )


def standardize_card_network(df: DataFrame) -> DataFrame:
    """Standardize card4 (network) and card6 (type) to lowercase."""
    result = df
    if "card4" in df.columns:
        result = result.withColumn("card4", lower(trim(col("card4"))))
    if "card6" in df.columns:
        result = result.withColumn("card6", lower(trim(col("card6"))))
    return result


def binarize_match_columns(df: DataFrame) -> DataFrame:
    """Convert M1–M9 from T/F strings to 1/0 integers."""
    result = df
    for i in range(1, 10):
        m_col = f"m{i}" if f"m{i}" in df.columns else f"M{i}"
        if m_col in df.columns:
            result = result.withColumn(m_col,
                when(col(m_col) == "T", lit(1))
                .when(col(m_col) == "F", lit(0))
                .otherwise(lit(None).cast(IntegerType()))
            )
    return result


def add_amount_features(df: DataFrame) -> DataFrame:
    """Pre-compute deterministic amount features."""
    return (df
        .withColumn("log_amount", log1p(col("transaction_amt")))
        .withColumn("amount_cents", (col("transaction_amt") * 100).cast("long") % 100)
        .withColumn("is_round_amount",
            when(col("transaction_amt") == floor(col("transaction_amt")), lit(1)).otherwise(lit(0)))
    )


def cast_types(df: DataFrame) -> DataFrame:
    """Explicit type casting for Silver."""
    string_cols = ["transaction_id", "card1", "card2", "card3", "card5"]
    result = df
    for c in string_cols:
        if c in df.columns:
            result = result.withColumn(c, col(c).cast(StringType()))
    return result
