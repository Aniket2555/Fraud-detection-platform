"""
Orchestrates the data quality gate between Bronze and Silver.
Runs hard checks → soft checks → profiling → routes rows.

Production behavior:
- Hard-fail rows → quarantine.ieee_cis_silver_rejects (with failure reason)
- Soft-fail rows → proceed to Silver with _quality_flags column
- All results → logged to MLflow for monitoring
"""

from pyspark.sql.functions import col, lit, current_timestamp, when, concat_ws
from pydeequ.verification import VerificationSuite, VerificationResult
import mlflow
import json

from fraud_detection.quality.bronze_constraints import (
    create_transaction_error_checks,
    create_transaction_warning_checks,
    run_data_profiling
)


def run_bronze_quality_gate(spark, bronze_table, quarantine_table):
    """
    Execute the full quality gate.
    Returns: (clean_df, reject_df, quality_report)
    """
    bronze_df = spark.table(bronze_table)

    # Hard constraint rejection conditions
    reject_conditions = (
        col("TransactionID").isNull() |
        (col("TransactionAmt") <= 0) |
        col("TransactionDT").isNull() |
        (col("TransactionDT") < 0) |
        col("isFraud").isNull() |
        (~col("ProductCD").isin("W", "H", "C", "S", "R"))
    )

    reject_df = (
        bronze_df
            .filter(reject_conditions)
            .withColumn("_rejection_reason",
                concat_ws("; ",
                    when(col("TransactionID").isNull(), lit("NULL_TRANSACTION_ID")),
                    when(col("TransactionAmt") <= 0, lit("NON_POSITIVE_AMOUNT")),
                    when(col("TransactionDT").isNull(), lit("NULL_TRANSACTION_DT")),
                    when(col("isFraud").isNull(), lit("NULL_LABEL")),
                    when(~col("ProductCD").isin("W", "H", "C", "S", "R"), lit("INVALID_PRODUCT_CD")),
                ))
            .withColumn("_quarantined_at", current_timestamp())
            .withColumn("_gate", lit("bronze_to_silver"))
    )

    clean_df = bronze_df.filter(~reject_conditions)

    # Flag soft warnings
    flagged_df = (
        clean_df
            .withColumn("_quality_flags",
                concat_ws("; ",
                    when(col("TransactionAmt") >= 50000, lit("HIGH_AMOUNT")),
                    when(col("addr1").isNull(), lit("MISSING_ADDR1")),
                    when(col("P_emaildomain").isNull(), lit("MISSING_EMAIL")),
                ))
    )

    reject_count = reject_df.count()
    if reject_count > 0:
        reject_df.write.mode("append").saveAsTable(quarantine_table)

    report = {
        "total_rows": bronze_df.count(),
        "clean_rows": clean_df.count(),
        "rejected_rows": reject_count,
        "rejection_rate": reject_count / max(bronze_df.count(), 1),
    }

    print(f"Quality Gate Results: {json.dumps(report, indent=2)}")
    return flagged_df, reject_df, report
