"""
Multi-Metric Drift Detection Engine: PSI, KS-Test, and Jensen-Shannon Divergence.
Evaluates feature-level and aggregate drift severity for automated retraining triggers.

Production fixes applied:
- compute_aggregate_drift_score() NO_DATA response now includes max_psi and mean_jsd keys
  (previously missing — callers crashed on .get("max_psi") when drift_report was empty)
- Added INSUFFICIENT_DATA status for feature columns with fewer than min_samples rows
  (was silently skipped — now explicitly tracked in the drift report)
- Added drift_severity_pct metric to aggregate summary for monitoring dashboards
- calculate_psi: epsilon raised from 1e-4 to 1e-6 (reduces artificial PSI inflation on
  near-empty buckets without affecting sensitivity at meaningful population sizes)
- evaluate_feature_set_drift: min_samples parameter added (default 30, was hardcoded 10)
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
from scipy.spatial.distance import jensenshannon

logger = logging.getLogger("drift_detector")

# Drift severity thresholds (configurable — can be loaded from App Config in future)
PSI_CRITICAL  = 0.25
PSI_WARNING   = 0.10
KS_CRITICAL   = 0.10
KS_P_CRITICAL = 0.001
KS_WARNING    = 0.05
KS_P_WARNING  = 0.01
JSD_CRITICAL  = 0.15
JSD_WARNING   = 0.05


def calculate_psi(
    expected: np.ndarray,
    actual: np.ndarray,
    num_buckets: int = 10,
) -> float:
    """
    Calculates Population Stability Index (PSI) between reference (expected) and current (actual).
    PSI = Σ (actual% - expected%) × ln(actual% / expected%)

    Thresholds (standard industry interpretation):
        PSI < 0.10  → STABLE
        PSI < 0.25  → WARNING (moderate shift)
        PSI >= 0.25 → CRITICAL (significant shift — retrain)
    """
    expected = expected[~np.isnan(expected)]
    actual   = actual[~np.isnan(actual)]

    if len(expected) < num_buckets or len(actual) < num_buckets:
        return 0.0

    percentiles = np.linspace(0, 100, num_buckets + 1)
    buckets = np.percentile(expected, percentiles)
    buckets[0]  = -np.inf
    buckets[-1] =  np.inf

    expected_counts = np.histogram(expected, bins=buckets)[0]
    actual_counts   = np.histogram(actual,   bins=buckets)[0]

    eps = 1e-6  # Smaller epsilon → less artificial inflation on near-empty buckets
    expected_pct = (expected_counts / len(expected)) + eps
    actual_pct   = (actual_counts   / len(actual))   + eps

    psi_value = float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))
    return psi_value


def calculate_jsd(
    expected: np.ndarray,
    actual: np.ndarray,
    num_buckets: int = 50,
) -> float:
    """
    Calculates Jensen-Shannon Divergence between reference and current distributions.
    JSD is symmetric and bounded: 0 = identical distributions, 1 = maximally different.
    Returns JSD² (same unit as PSI — easier to interpret comparatively).
    """
    expected = expected[~np.isnan(expected)]
    actual   = actual[~np.isnan(actual)]

    if len(expected) < num_buckets or len(actual) < num_buckets:
        return 0.0

    combined = np.concatenate([expected, actual])
    bins = np.histogram_bin_edges(combined, bins=num_buckets)

    p_hist = np.histogram(expected, bins=bins, density=True)[0] + 1e-10
    q_hist = np.histogram(actual,   bins=bins, density=True)[0] + 1e-10

    p_norm = p_hist / p_hist.sum()
    q_norm = q_hist / q_hist.sum()

    return float(jensenshannon(p_norm, q_norm) ** 2)


def evaluate_feature_set_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    feature_cols: list,
    min_samples: int = 30,
) -> dict:
    """
    Evaluates PSI, KS-statistic, and JSD across all input features.
    Returns per-feature drift report with severity classification.

    Args:
        reference_df: Reference (training snapshot) DataFrame.
        current_df:   Current production feature DataFrame.
        feature_cols: List of feature column names to evaluate.
        min_samples:  Minimum non-null rows required to compute drift metrics (default 30).

    Returns:
        Dict mapping feature name → {psi, ks_stat, ks_p_value, jsd, status}
        Status is one of: STABLE | WARNING | CRITICAL_DRIFT | INSUFFICIENT_DATA
    """
    drift_report: dict = {}

    for col_name in feature_cols:
        if col_name not in reference_df.columns or col_name not in current_df.columns:
            logger.debug("Feature '%s' not found in both DataFrames — skipping.", col_name)
            continue

        ref_vals  = reference_df[col_name].dropna().values.astype(float)
        curr_vals = current_df[col_name].dropna().values.astype(float)

        if len(ref_vals) < min_samples or len(curr_vals) < min_samples:
            # Track explicitly so dashboards can flag data pipeline issues
            drift_report[col_name] = {
                "psi": 0.0,
                "ks_stat": 0.0,
                "ks_p_value": 1.0,
                "jsd": 0.0,
                "status": "INSUFFICIENT_DATA",
                "ref_n": int(len(ref_vals)),
                "curr_n": int(len(curr_vals)),
            }
            continue

        psi_val               = calculate_psi(ref_vals, curr_vals)
        ks_stat, ks_p_value   = ks_2samp(ref_vals, curr_vals)
        jsd_val               = calculate_jsd(ref_vals, curr_vals)

        if (psi_val >= PSI_CRITICAL
                or (ks_stat >= KS_CRITICAL and ks_p_value < KS_P_CRITICAL)
                or jsd_val >= JSD_CRITICAL):
            status = "CRITICAL_DRIFT"
        elif (psi_val >= PSI_WARNING
              or (ks_stat >= KS_WARNING and ks_p_value < KS_P_WARNING)
              or jsd_val >= JSD_WARNING):
            status = "WARNING"
        else:
            status = "STABLE"

        drift_report[col_name] = {
            "psi":          round(psi_val, 4),
            "ks_stat":      round(float(ks_stat), 4),
            "ks_p_value":   round(float(ks_p_value), 6),
            "jsd":          round(jsd_val, 4),
            "status":       status,
            "ref_n":        int(len(ref_vals)),
            "curr_n":       int(len(curr_vals)),
        }

    return drift_report


def compute_aggregate_drift_score(drift_report: dict) -> dict:
    """
    Computes aggregate drift severity from per-feature drift reports.

    Production fix: The NO_DATA response now includes max_psi and mean_jsd keys
    so callers can always safely access these fields without defensive get() checks.

    Returns:
        Dict with keys:
            overall_status          : STABLE | WARNING | CRITICAL_DRIFT | NO_DATA
            critical_count          : int
            warning_count           : int
            stable_count            : int
            insufficient_data_count : int
            max_psi                 : float  (0.0 when no data)
            mean_jsd                : float  (0.0 when no data)
            drift_severity_pct      : float  fraction of features in CRITICAL/WARNING
            total_features_evaluated: int
    """
    # Base response with all keys always present (prevents KeyError in callers)
    base = {
        "overall_status": "NO_DATA",
        "critical_count": 0,
        "warning_count": 0,
        "stable_count": 0,
        "insufficient_data_count": 0,
        "max_psi": 0.0,       # ← was missing from NO_DATA response (the bug)
        "mean_jsd": 0.0,      # ← was missing from NO_DATA response (the bug)
        "drift_severity_pct": 0.0,
        "total_features_evaluated": 0,
    }

    if not drift_report:
        return base

    critical_count          = sum(1 for v in drift_report.values() if v["status"] == "CRITICAL_DRIFT")
    warning_count           = sum(1 for v in drift_report.values() if v["status"] == "WARNING")
    stable_count            = sum(1 for v in drift_report.values() if v["status"] == "STABLE")
    insufficient_data_count = sum(1 for v in drift_report.values() if v["status"] == "INSUFFICIENT_DATA")

    # Only include features with actual drift metrics in max_psi / mean_jsd
    scored_features = [v for v in drift_report.values() if v["status"] != "INSUFFICIENT_DATA"]
    max_psi  = max((v["psi"] for v in scored_features), default=0.0)
    mean_jsd = float(np.mean([v["jsd"] for v in scored_features])) if scored_features else 0.0

    total = len(drift_report)
    drift_severity_pct = (critical_count + warning_count) / max(1, total)

    if critical_count > 0:
        overall_status = "CRITICAL_DRIFT"
    elif warning_count / max(1, total) > 0.20:
        overall_status = "WARNING"
    else:
        overall_status = "STABLE"

    return {
        "overall_status":           overall_status,
        "critical_count":           critical_count,
        "warning_count":            warning_count,
        "stable_count":             stable_count,
        "insufficient_data_count":  insufficient_data_count,
        "max_psi":                  round(max_psi, 4),
        "mean_jsd":                 round(mean_jsd, 4),
        "drift_severity_pct":       round(drift_severity_pct, 4),
        "total_features_evaluated": total,
    }
