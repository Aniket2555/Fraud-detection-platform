# Databricks notebook
# Bronze Streaming Ingestion: Event Hubs → Delta

import json
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

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

consumer_conn_str = dbutils.secrets.get("kv-fraud", "eventhub-consumer-conn-str")

eh_conf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(consumer_conn_str),
    "eventhubs.consumerGroup": "bronze-ingest",
    "eventhubs.startingPosition": json.dumps({
        "offset": "-1",
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True
    }),
    "maxEventsPerTrigger": "10000",
}

raw_stream = (
    spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
)

parsed_stream = (
    raw_stream
    .withColumn("body_str", col("body").cast("string"))
    .withColumn("parsed", from_json(col("body_str"), TRANSACTION_EVENT_SCHEMA_V1))
    .select(
        col("parsed.*"),
        col("enqueuedTime").alias("_enqueued_time"),
        col("offset").alias("_eh_offset"),
        col("partition").cast("string").alias("_source_partition"),
        current_timestamp().alias("_ingested_at"),
        col("parsed").isNotNull().alias("_parse_success"),
        col("body_str").alias("_raw_body"),
    )
)

success_stream = (
    parsed_stream
    .filter(col("_parse_success") == True)
    .drop("_parse_success", "_raw_body")
    .withColumn("event_date", to_date(col("event_time")))
)

failure_stream = (
    parsed_stream
    .filter(col("_parse_success") == False)
    .select(
        col("_raw_body"),
        col("_enqueued_time"),
        col("_source_partition"),
        col("_ingested_at"),
        lit("JSON_PARSE_FAILURE").alias("failure_reason"),
    )
)

checkpoint_path = "abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/bronze_streaming_txn/"

bronze_query = (
    success_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .partitionBy("event_date")
    .trigger(processingTime="10 seconds")
    .toTable("fraud_detection_dev.bronze.transactions")
)

dlq_checkpoint = "abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/bronze_dlq/"

dlq_query = (
    failure_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", dlq_checkpoint)
    .trigger(processingTime="30 seconds")
    .toTable("fraud_detection_dev.quarantine.bronze_stream_parse_failures")
)

print(f"Bronze streaming query started: {bronze_query.id}")
