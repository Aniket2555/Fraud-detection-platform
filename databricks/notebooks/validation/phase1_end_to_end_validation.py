# Databricks notebook
# Phase 1 End-to-End Validation Suite

from pyspark.sql.functions import col, avg

print("==========================================")
echo_title = "Phase 1 End-to-End Validation Suite"
print(echo_title)
print("==========================================")

# 1. Bronze Validation
bronze_txn = spark.table("fraud_detection_dev.bronze.ieee_cis_transactions").count()
bronze_id = spark.table("fraud_detection_dev.bronze.ieee_cis_identity").count()
print(f"Bronze Transactions Count: {bronze_txn}")
print(f"Bronze Identity Count: {bronze_id}")
assert bronze_txn > 0, "Bronze transactions table is empty!"

# 2. Silver Validation
silver = spark.table("fraud_detection_dev.silver.transactions")
silver_count = silver.count()
fraud_rate = silver.filter(col("is_fraud") == 1).count() / silver_count
print(f"Silver Transactions Count: {silver_count}")
print(f"Silver Fraud Rate: {fraud_rate:.4f}")
assert silver_count > 0, "Silver transactions table is empty!"

# 3. Gold Validation
gold_daily = spark.table("fraud_detection_dev.gold.daily_fraud_summary").count()
gold_product = spark.table("fraud_detection_dev.gold.product_risk_scores").count()
print(f"Gold Daily Summary Rows: {gold_daily}")
print(f"Gold Product Risk Rows: {gold_product}")
assert gold_daily > 0, "Gold daily summary table is empty!"

print("==========================================")
print("✅ ALL PHASE 1 END-TO-END CHECKS PASSED!")
print("==========================================")
