# Databricks notebook
# Bronze ingestion: IEEE-CIS train_identity.csv → bronze.ieee_cis_identity

from pyspark.sql.functions import current_timestamp, input_file_name, lit
import uuid

ADLS_ACCOUNT = dbutils.secrets.get("kv-fraud", "adls-account-name")
RAW_PATH = f"abfss://raw@{ADLS_ACCOUNT}.dfs.core.windows.net/ieee-cis/"
CHECKPOINT_PATH = f"abfss://checkpoints@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze/ieee_cis_identity/"
SCHEMA_LOCATION = f"{CHECKPOINT_PATH}_schema/"
BATCH_ID = str(uuid.uuid4())

raw_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("header", "true")
        .option("nullValue", "")
        .option("badRecordsPath", f"abfss://quarantine@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze_parse_failures/")
        .option("pathGlobFilter", "train_identity.csv")
        .load(RAW_PATH)
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
    .toTable("fraud_detection_dev.bronze.ieee_cis_identity"))

spark.sql("""
    ALTER TABLE fraud_detection_dev.bronze.ieee_cis_identity 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

count = spark.table("fraud_detection_dev.bronze.ieee_cis_identity").count()
print(f"Bronze ieee_cis_identity: {count} rows ingested")
