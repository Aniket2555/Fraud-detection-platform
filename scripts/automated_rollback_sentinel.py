"""
Automated Rollback Sentinel.
Queries live telemetry from Gold tables and reverts model registry version
if production anomalies exceed safety thresholds.
"""

import mlflow
import os
import sys
from datetime import datetime

FPR_MULTIPLIER_THRESHOLD = 2.0
LATENCY_P99_THRESHOLD_MS = 100.0
MIN_BAKE_HOURS = 4


def query_production_telemetry(spark) -> dict:
    """Queries live model telemetry from Gold KPI tables."""
    recent = spark.sql("""
        SELECT
            SUM(false_positives) / GREATEST(1, SUM(false_positives + true_negatives)) as recent_fpr,
            SUM(true_positives) / GREATEST(1, SUM(true_positives + false_negatives)) as recent_recall,
            AVG(avg_fraud_score) as recent_avg_score
        FROM fraud_detection_dev.gold.model_performance_kpis
        WHERE kpi_date >= current_date() - 1
    """).collect()[0]

    baseline = spark.sql("""
        SELECT
            SUM(false_positives) / GREATEST(1, SUM(false_positives + true_negatives)) as baseline_fpr,
            SUM(true_positives) / GREATEST(1, SUM(true_positives + false_negatives)) as baseline_recall
        FROM fraud_detection_dev.gold.model_performance_kpis
        WHERE kpi_date BETWEEN current_date() - 31 AND current_date() - 1
    """).collect()[0]

    drift = spark.sql("""
        SELECT feature_drift_status, concept_drift_status
        FROM fraud_detection_dev.gold.drift_monitoring_history
        ORDER BY check_date DESC LIMIT 1
    """).collect()

    return {
        "recent_fpr": float(recent["recent_fpr"] or 0),
        "baseline_fpr": float(baseline["baseline_fpr"] or 0),
        "recent_recall": float(recent["recent_recall"] or 0),
        "baseline_recall": float(baseline["baseline_recall"] or 0),
        "recent_avg_score": float(recent["recent_avg_score"] or 0),
        "latest_drift_status": drift[0]["feature_drift_status"] if drift else "UNKNOWN"
    }


def check_and_rollback(spark):
    """Evaluates production health and triggers rollback if anomalies detected."""
    telemetry = query_production_telemetry(spark)

    rollback_triggered = False
    reasons = []

    if telemetry["baseline_fpr"] > 0 and telemetry["recent_fpr"] > (telemetry["baseline_fpr"] * FPR_MULTIPLIER_THRESHOLD):
        rollback_triggered = True
        reasons.append(f"FPR spiked to {telemetry['recent_fpr']:.4f} (baseline {telemetry['baseline_fpr']:.4f}, threshold {FPR_MULTIPLIER_THRESHOLD}x)")

    if telemetry["baseline_recall"] > 0 and telemetry["recent_recall"] < (telemetry["baseline_recall"] * 0.80):
        rollback_triggered = True
        reasons.append(f"Recall collapsed to {telemetry['recent_recall']:.4f} (baseline {telemetry['baseline_recall']:.4f})")

    if rollback_triggered:
        print(f"🚨 ROLLBACK TRIGGERED! Reasons: {reasons}")
        client = mlflow.tracking.MlflowClient()

        prod_versions = client.get_latest_versions("fraud-ensemble-champion", stages=["Production"])
        # NOTE: get_latest_versions(stages=["Archived"]) returns the version
        # with the highest *version number* currently archived, not the one
        # most recently demoted from Production. Across repeated rollback
        # cycles those diverge: a newer bad model archived later can have a
        # lower version number than an older bad model archived earlier,
        # which would revert production to an already-proven-bad model.
        # search_model_versions + sorting by last_updated_timestamp picks
        # the version that was *most recently* moved to Archived, which is
        # the version that was actually in Production immediately before
        # the current one (promotions auto-archive the prior Production
        # version via archive_existing_versions=True below).
        archived_versions = sorted(
            (
                v for v in client.search_model_versions("name='fraud-ensemble-champion'")
                if v.current_stage == "Archived"
            ),
            key=lambda v: v.last_updated_timestamp,
            reverse=True,
        )

        if prod_versions and archived_versions:
            current_ver = prod_versions[0].version
            prev_ver = archived_versions[0].version

            print(f"Reverting from Version {current_ver} to Version {prev_ver}...")
            client.transition_model_version_stage(
                name="fraud-ensemble-champion",
                version=prev_ver,
                stage="Production",
                archive_existing_versions=True
            )
            print(f"✅ ROLLBACK COMPLETED. Active version: {prev_ver}")
        else:
            print("⚠️ No archived version available for rollback. Manual intervention required.")
    else:
        print(f"✅ Production healthy. FPR={telemetry['recent_fpr']:.4f}, Recall={telemetry['recent_recall']:.4f}")
