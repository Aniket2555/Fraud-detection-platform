# Databricks notebook
# Post-Silver Validation Notebook

from pyspark.sql.functions import col
from fraud_detection.quality.silver_constraints import create_silver_post_transform_checks

SILVER_TABLE = "fraud_detection_dev.silver.transactions"
silver_df = spark.table(SILVER_TABLE)

count = silver_df.count()
print(f"Validating Silver Table: {count} total rows")

assert count > 0, "Silver table is empty!"

# Verify unique transaction_id
unique_ids = silver_df.select("transaction_id").distinct().count()
assert unique_ids == count, f"Duplicate transaction_ids found: {count - unique_ids}"

# Check fraud rate. This is a WARNING, not a hard assert: silver.transactions
# blends sources with very different natural fraud prevalence (IEEE-CIS batch
# ~3.5%, Kaggle streaming replay ~0.17%), so a fixed [0.02, 0.06] bound on the
# full table would false-fail validation the moment streaming data lands.
fraud_rate = silver_df.filter(col("is_fraud") == 1).count() / count
print(f"Silver Fraud Rate: {fraud_rate:.4f}")
if not (0.0001 <= fraud_rate <= 0.10):
    print(f"⚠️  WARNING: Silver fraud rate {fraud_rate:.4f} outside the broad sanity band [0.0001, 0.10] — investigate.")

print("✅ Silver Post-Transformation Validation Passed!")
