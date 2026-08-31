"""
Automated Rollback Sentinel.
Queries live telemetry from Gold tables and reverts model registry version
if production anomalies exceed safety thresholds.

Production fixes applied:
- This workspace's model registry is Unity Catalog (3-level names + aliases), not
  the legacy workspace registry (stages, get_latest_versions(stages=...)) the plan
  assumed. UC ModelVersion objects have no current_stage/last_updated_timestamp --
  "rollback" here means reassigning the @champion alias back to the previous
  version, determined by version number (this pipeline creates exactly one new
  model version per retrain and only ever promotes that new version or leaves the
  existing one, so "the previous version by number" is equivalent to "the version
  that was @champion immediately before the current one").
"""

import mlflow

FPR_MULTIPLIER_THRESHOLD = 2.0
LATENCY_P99_THRESHOLD_MS = 100.0
MIN_BAKE_HOURS = 4

MODEL_NAME = "fraud_detection_dev.gold.fraud_ensemble_champion"


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
    mlflow.set_registry_uri("databricks-uc")
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
        print(f"ROLLBACK TRIGGERED! Reasons: {reasons}")
        client = mlflow.tracking.MlflowClient()

        rm = client.get_registered_model(MODEL_NAME)
        current_champion_version = int(rm.aliases.get("champion", -1))
        if current_champion_version < 0:
            print("No @champion alias set at all. Manual intervention required.")
            return

        all_versions = sorted(
            (int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'")),
            reverse=True,
        )
        prior_versions = [v for v in all_versions if v < current_champion_version]

        if prior_versions:
            prev_ver = prior_versions[0]
            print(f"Reverting @champion from version {current_champion_version} to version {prev_ver}...")
            client.set_registered_model_alias(MODEL_NAME, "champion", prev_ver)
            print(f"ROLLBACK COMPLETED. @champion now points to version {prev_ver}.")
        else:
            print("No prior version available for rollback. Manual intervention required.")
    else:
        print(f"Production healthy. FPR={telemetry['recent_fpr']:.4f}, Recall={telemetry['recent_recall']:.4f}")
