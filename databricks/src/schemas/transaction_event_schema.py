"""
Spark StructType for the canonical TransactionEvent.
Used by the Bronze streaming consumer to parse JSON from Event Hubs.
Must be kept in sync with schemas/transaction_event_v1.json.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType
)

TRANSACTION_EVENT_SCHEMA_V1 = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("ingestion_time", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_category", StringType(), True),
    StructField("payment_method", StringType(), False),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("billing_country", StringType(), True),
    StructField("shipping_country", StringType(), True),
    StructField("channel", StringType(), False),
    StructField("is_recurring", BooleanType(), True),
    StructField("session_id", StringType(), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("is_synthetic", BooleanType(), True),
    StructField("source", StringType(), True),
])
