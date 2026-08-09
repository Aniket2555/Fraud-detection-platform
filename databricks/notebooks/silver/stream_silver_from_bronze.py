# Databricks notebook
# Silver Streaming: Bronze (streaming) → Silver

from pyspark.sql.functions import (
    col, lower, trim, when, to_timestamp, hour,
    dayofweek, current_timestamp, lit
)
from delta.tables import DeltaTable

bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table("fraud_detection_dev.bronze.transactions")
)

def apply_silver_transforms(df):
    return (
        df
        .withColumn("event_time_ts", to_timestamp(col("event_time")))
        .withColumn("merchant_category", lower(trim(col("merchant_category"))))
        .withColumn("payment_method", lower(trim(col("payment_method"))))
        .withColumn("channel", lower(trim(col("channel"))))
        .withColumn("billing_country", lower(trim(col("billing_country"))))
        .withColumn("device_id", when(col("device_id").isNull(), lit("unknown")).otherwise(col("device_id")))
        .withColumn("shipping_country", when(col("shipping_country").isNull(), col("billing_country")).otherwise(col("shipping_country")))
        .withColumn("latitude", when((col("latitude").between(-90, 90)), col("latitude")).otherwise(lit(None)))
        .withColumn("longitude", when((col("longitude").between(-180, 180)), col("longitude")).otherwise(lit(None)))
        .filter(col("amount") >= 0)
        .withColumn("hour_of_day", hour(col("event_time_ts")))
        .withColumn("day_of_week", dayofweek(col("event_time_ts")))
        .withColumn("_silver_processed_at", current_timestamp())
    )

silver_stream = apply_silver_transforms(bronze_stream)

def merge_to_silver(batch_df, batch_id):
    silver_table = DeltaTable.forName(spark, "fraud_detection_dev.silver.transactions")
    (silver_table.alias("target")
        .merge(batch_df.alias("source"), "target.transaction_id = source.transaction_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

silver_checkpoint = "abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/silver_streaming_txn/"

silver_query = (
    silver_stream.writeStream
    .foreachBatch(merge_to_silver)
    .option("checkpointLocation", silver_checkpoint)
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"Silver streaming merge started: {silver_query.id}")
