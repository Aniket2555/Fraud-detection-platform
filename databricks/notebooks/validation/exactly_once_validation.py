# Databricks notebook
# Exactly-Once Semantics Validation

from pyspark.sql.functions import countDistinct, count, col

bronze_df = spark.table("fraud_detection_dev.bronze.transactions")

total_rows = bronze_df.count()
distinct_txn_ids = bronze_df.select(countDistinct("transaction_id")).collect()[0][0]
duplicate_count = total_rows - distinct_txn_ids

print(f"Total rows in bronze.transactions: {total_rows:,}")
print(f"Distinct transaction_ids:           {distinct_txn_ids:,}")
print(f"Duplicate rows:                     {duplicate_count:,}")

if duplicate_count == 0:
    print("✅ EXACTLY-ONCE VALIDATED: No duplicates in Bronze.")
else:
    print(f"⚠️ WARNING: {duplicate_count} duplicate rows detected.")
