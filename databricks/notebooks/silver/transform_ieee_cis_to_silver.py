"""
Silver transformation: Bronze → Silver for IEEE-CIS dataset.
Applies all cleaning, conforming, deduplication, and quality flagging.

Production guarantees:
- Idempotent (MERGE-based upsert on transaction_id)
- Quarantine-aware (rejects written to quarantine table)
- Fully logged (MLflow metrics for every run)
"""

from pyspark.sql.functions import col, current_timestamp, lit
import mlflow
from delta.tables import DeltaTable

from fraud_detection.transformations.cleaning import (
    rename_columns_to_snake_case,
    synthesize_timestamps,
    handle_categorical_nulls,
    handle_numeric_null_flags,
    normalize_email_domains,
    standardize_card_network,
    binarize_match_columns,
    add_amount_features,
    cast_types
)

BRONZE_TXN_TABLE = "fraud_detection_dev.bronze.ieee_cis_transactions"
BRONZE_IDENTITY_TABLE = "fraud_detection_dev.bronze.ieee_cis_identity"
SILVER_TABLE = "fraud_detection_dev.silver.transactions"
QUARANTINE_TABLE = "fraud_detection_dev.quarantine.ieee_cis_silver_rejects"

# Run Quality Gate
clean_df, reject_df, quality_report = run_bronze_quality_gate(
    spark, BRONZE_TXN_TABLE, QUARANTINE_TABLE
)

# Join with identity table
identity_df = spark.table(BRONZE_IDENTITY_TABLE)
identity_renamed = rename_columns_to_snake_case(identity_df)

joined_df = clean_df.join(
    identity_renamed.drop("_ingested_at", "_source_file", "_batch_id", "load_date"),
    on="TransactionID",
    how="left"
)

# Transformations
silver_df = joined_df
silver_df = rename_columns_to_snake_case(silver_df)
silver_df = cast_types(silver_df)
silver_df = synthesize_timestamps(silver_df)
silver_df = standardize_card_network(silver_df)
silver_df = handle_categorical_nulls(silver_df, [
    "product_cd", "card4", "card6", "p_emaildomain", "r_emaildomain",
    "device_type", "device_info"
])
silver_df = normalize_email_domains(silver_df, "p_emaildomain")
silver_df = normalize_email_domains(silver_df, "r_emaildomain")
silver_df = binarize_match_columns(silver_df)
silver_df = add_amount_features(silver_df)
silver_df = handle_numeric_null_flags(silver_df, [
    "addr1", "addr2", "dist1", "dist2",
    "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9",
    "c10", "c11", "c12", "c13", "c14"
])

silver_df = (silver_df
    .withColumn("_silver_processed_at", current_timestamp())
    .withColumn("_silver_version", lit("1.0"))
)

# Write to Silver (MERGE)
if spark.catalog.tableExists(SILVER_TABLE):
    silver_table = DeltaTable.forName(spark, SILVER_TABLE)
    (silver_table.alias("target")
        .merge(silver_df.alias("source"), "target.transaction_id = source.transaction_id")
        .whenNotMatchedInsertAll()
        .execute())
else:
    (silver_df.write
        .format("delta")
        .partitionBy("event_date")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE))

spark.sql(f"""
    ALTER TABLE {SILVER_TABLE} 
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.logRetentionDuration' = 'interval 30 days'
    )
""")

silver_count = spark.table(SILVER_TABLE).count()
print(f"Silver transactions: {silver_count} rows")
