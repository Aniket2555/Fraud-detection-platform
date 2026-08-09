# Databricks notebook
# Bronze ingestion: IEEE-CIS train_transaction.csv → bronze.ieee_cis_transactions

from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid

ADLS_ACCOUNT = dbutils.secrets.get("kv-fraud", "adls-account-name")
RAW_PATH = f"abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/ieee-cis/"
CHECKPOINT_PATH = f"abfss://checkpoints@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze/ieee_cis_transactions/"
BATCH_ID = str(uuid.uuid4())

schema_hints = {
    "cloudFiles.schemaHints": "TransactionID INT, TransactionDT INT, TransactionAmt DOUBLE, isFraud INT"
}

raw_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("multiLine", "false")
        .option("escape", '"')
        .option("nullValue", "")
        .option("cloudFiles.badRecordsPath", f"abfss://quarantine@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze_parse_failures/")
        .options(**schema_hints)
        .load(f"{RAW_PATH}train_transaction.csv")
)

bronze_df = (
    raw_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(BATCH_ID))
        .withColumn("load_date", current_timestamp().cast("date"))
)

(bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .partitionBy("load_date")
    .trigger(availableNow=True)
    .toTable("fraud_detection_dev.bronze.ieee_cis_transactions"))

spark.sql("""
    ALTER TABLE fraud_detection_dev.bronze.ieee_cis_transactions 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.logRetentionDuration' = 'interval 30 days',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")

count = spark.table("fraud_detection_dev.bronze.ieee_cis_transactions").count()
print(f"Bronze ieee_cis_transactions: {count} rows ingested")
