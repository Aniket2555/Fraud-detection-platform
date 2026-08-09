"""
Concept Drift Detector: Monitors prediction score distribution shift
and actual fraud rate deviation over time windows.
"""

import numpy as np
from fraud_detection.mlops.drift_detector import calculate_psi, calculate_jsd


def detect_prediction_drift(
    baseline_scores: np.ndarray,
    current_scores: np.ndarray,
    psi_threshold: float = 0.15,
    jsd_threshold: float = 0.10
) -> dict:
    """
    Detects concept drift by comparing model prediction score distributions.
    A shift in prediction distribution without feature drift suggests concept drift.
    """
    psi = calculate_psi(baseline_scores, current_scores)
    jsd = calculate_jsd(baseline_scores, current_scores)

    if psi >= psi_threshold or jsd >= jsd_threshold:
        status = "CONCEPT_DRIFT_DETECTED"
    else:
        status = "STABLE"

    return {
        "prediction_psi": round(psi, 4),
        "prediction_jsd": round(jsd, 4),
        "status": status,
        "baseline_mean": round(float(np.mean(baseline_scores)), 4),
        "current_mean": round(float(np.mean(current_scores)), 4),
        "baseline_std": round(float(np.std(baseline_scores)), 4),
        "current_std": round(float(np.std(current_scores)), 4)
    }


def detect_label_rate_drift(
    baseline_fraud_rate: float,
    current_fraud_rate: float,
    relative_change_threshold: float = 0.30
) -> dict:
    """
    Detects sudden shifts in the observed fraud rate (ground truth label distribution).
    A >30% relative change in fraud rate suggests adversarial pattern evolution.
    """
    if baseline_fraud_rate > 0:
        relative_change = abs(current_fraud_rate - baseline_fraud_rate) / baseline_fraud_rate
    else:
        relative_change = float(current_fraud_rate > 0)

    status = "LABEL_DRIFT_DETECTED" if relative_change >= relative_change_threshold else "STABLE"

    return {
        "baseline_fraud_rate": round(baseline_fraud_rate, 4),
        "current_fraud_rate": round(current_fraud_rate, 4),
        "relative_change": round(relative_change, 4),
        "status": status
    }
