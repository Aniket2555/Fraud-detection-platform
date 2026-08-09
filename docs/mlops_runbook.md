# MLOps Operational Runbook

## 1. Daily Drift Monitoring
- **Schedule:** 06:00 IST daily via `mlops_daily_drift_and_retrain` Databricks Workflow.
- **Metrics:** PSI, KS-Statistic, Jensen-Shannon Divergence (per-feature + aggregate).
- **Thresholds:** PSI ≥ 0.25 = CRITICAL (auto-retrain); PSI ≥ 0.10 = WARNING (monitor).
- **Dashboard:** Query `fraud_detection_dev.gold.drift_monitoring_history` in Databricks SQL.

## 2. Retraining Triggers
| Trigger | Condition | Action |
|---|---|---|
| Scheduled | Every 2 weeks | Auto-retrain full ensemble |
| Drift | PSI ≥ 0.25 on any feature | Auto-retrain triggered by drift check |
| Concept Drift | Prediction PSI ≥ 0.15 | Auto-retrain triggered by drift check |
| Manual | On-demand via `retrain_pipeline.py` | Engineer triggers with custom reason |

## 3. Champion vs. Challenger Gates
All 4 gates must pass for automatic promotion:
1. PR-AUC non-regression (tolerance: -0.005)
2. High-value (>$5K) recall non-regression (tolerance: -1%)
3. FPR cap (max 5% increase)
4. Latency p99 ≤ 100ms

## 4. Rollback Procedure
- **Automatic:** `automated_rollback_sentinel.py` monitors FPR (>2x baseline) and recall collapse (<80% baseline).
- **Manual:** `mlflow.tracking.MlflowClient().transition_model_version_stage(name="fraud-ensemble-champion", version=<prev>, stage="Production")`

## 5. Shadow Scoring Analysis
Query `gold.shadow_scoring_logs` to compare Champion vs. Challenger:
```sql
SELECT AVG(score_delta), STDDEV(score_delta), MAX(ABS(score_delta))
FROM fraud_detection_dev.gold.shadow_scoring_logs
WHERE _shadow_scored_at >= current_date() - 7
```
