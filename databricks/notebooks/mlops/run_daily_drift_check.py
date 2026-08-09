# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Automated Feature & Concept Drift Monitoring Job
# MAGIC Evaluates all 6 feature families + prediction distribution + label rate drift.

import json
import pandas as pd
from pyspark.sql.functions import current_timestamp, current_date, lit
from drift_detector import evaluate_feature_set_drift, compute_aggregate_drift_score
from concept_drift_detector import detect_prediction_drift, detect_label_rate_drift

ref_df = spark.table("fraud_detection_dev.gold.train_feature_snapshot").toPandas()

curr_velocity = spark.table("fraud_detection_dev.gold.feature_card_velocity").filter(
    "feature_timestamp >= current_date() - 1"
).toPandas()

feature_columns = [c for c in ref_df.columns if c.startswith((
    "vel_", "geo_", "feature_", "base_", "merch_", "graph_"
))]

drift_results = evaluate_feature_set_drift(ref_df, curr_velocity, feature_columns)
aggregate = compute_aggregate_drift_score(drift_results)

print(f"Feature Drift Summary: {json.dumps(aggregate, indent=2)}")

baseline_scores = spark.table("fraud_detection_dev.gold.train_prediction_baseline").select(
    "fraud_probability"
).toPandas()["fraud_probability"].values

current_scores = spark.table("fraud_detection_dev.gold.decision_audit_log").filter(
    "timestamp >= current_date() - 1"
).select("fraud_score").toPandas()["fraud_score"].values

concept_result = detect_prediction_drift(baseline_scores, current_scores)
print(f"Concept Drift: {json.dumps(concept_result, indent=2)}")

kpi_df = spark.table("fraud_detection_dev.gold.model_performance_kpis")
baseline_fraud_rate = kpi_df.filter("kpi_date <= current_date() - 30").select(
    "actual_fraud_count", "total_scored_labeled"
).toPandas()

if len(baseline_fraud_rate) > 0:
    bl_rate = baseline_fraud_rate["actual_fraud_count"].sum() / max(1, baseline_fraud_rate["total_scored_labeled"].sum())
    recent_kpis = kpi_df.filter("kpi_date >= current_date() - 7").select(
        "actual_fraud_count", "total_scored_labeled"
    ).toPandas()
    curr_rate = recent_kpis["actual_fraud_count"].sum() / max(1, recent_kpis["total_scored_labeled"].sum())
    label_result = detect_label_rate_drift(bl_rate, curr_rate)
    print(f"Label Rate Drift: {json.dumps(label_result, indent=2)}")
else:
    label_result = {"status": "INSUFFICIENT_DATA"}

drift_log_df = spark.createDataFrame([{
    "check_date": str(pd.Timestamp.now().date()),
    "feature_drift_status": aggregate["overall_status"],
    "max_psi": aggregate["max_psi"],
    "mean_jsd": aggregate["mean_jsd"],
    "critical_features": aggregate["critical_count"],
    "warning_features": aggregate["warning_count"],
    "concept_drift_status": concept_result["status"],
    "prediction_psi": concept_result["prediction_psi"],
    "label_drift_status": label_result.get("status", "N/A"),
    "drift_report_json": json.dumps(drift_results)
}])
drift_log_df.write.format("delta").mode("append").saveAsTable("fraud_detection_dev.gold.drift_monitoring_history")

should_retrain = (
    aggregate["overall_status"] == "CRITICAL_DRIFT" or
    concept_result["status"] == "CONCEPT_DRIFT_DETECTED" or
    label_result.get("status") == "LABEL_DRIFT_DETECTED"
)

if should_retrain:
    trigger_reason = f"DRIFT_{aggregate['overall_status']}_CONCEPT_{concept_result['status']}"
    print(f"🚨 TRIGGERING RETRAINING: {trigger_reason}")
    dbutils.notebook.run(
        "/Repos/fraud-detection/databricks/notebooks/mlops/retrain_pipeline",
        3600,
        {"trigger_reason": trigger_reason}
    )
else:
    print("✅ All drift metrics stable. No retraining required.")
