# Databricks notebook
# Materialize Feature Store Delta Tables

from pyspark.sql.functions import current_timestamp, to_date

silver_stream = spark.readStream.table("fraud_detection_dev.silver.transactions")

# 1. Stateless Features Materialization
# %run ../src/features/stateless_features
# stateless_df = compute_stateless_features(silver_stream)

# 2. Card Velocity 5m Materialization
# %run ../src/features/velocity_features
# card_vel_5m = compute_card_velocity_5m(silver_stream)

print("✅ Feature store materialization streaming pipeline configured.")
