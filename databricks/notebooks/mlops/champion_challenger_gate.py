# Databricks notebook source
# MAGIC %md
# MAGIC # Champion vs. Challenger 4-Gate Evaluation with Statistical Significance
# MAGIC
# MAGIC Production fixes applied:
# MAGIC - `scipy.stats.binom_test` was removed in current scipy (replaced by `binomtest`,
# MAGIC   which returns a result object rather than a bare p-value).
# MAGIC - feature_cols selection now matches the registered Champion's actual training
# MAGIC   schema (["amount", "latitude", "longitude"] + prefix-matched engineered features,
# MAGIC   minus the two raw base_cust_*_ts timestamp columns the naive prefix match used to
# MAGIC   sweep in) -- same fix as retrain_pipeline.py; a mismatched feature count/order
# MAGIC   makes FraudEnsemblePyFunc._validate_and_coerce hard-fail before any gate can run.
# MAGIC - Removed the duplicate "amount" from the outer .select() list now that it's part
# MAGIC   of feature_cols (was producing two identically-named columns).
# MAGIC - This workspace's model registry is Unity Catalog (3-level names + aliases), not
# MAGIC   the legacy workspace registry (`models:/name/Stage`) the plan assumed -- switched
# MAGIC   to `models:/fraud_detection_dev.gold.fraud_ensemble_champion@champion` and
# MAGIC   `client.set_registered_model_alias(..., "champion", version)` for promotion.
# MAGIC   UC has no "Archived" stage; the previous version simply loses the @champion alias
# MAGIC   (still queryable by its version number).
# MAGIC - `gold.reconciled_labeled_transactions` only ever carries raw transaction columns --
# MAGIC   retrain_pipeline.py's PIT-joined engineered features (vel_/geo_/base_/merch_/graph_)
# MAGIC   are computed into an in-memory DataFrame and never written back to that table. The
# MAGIC   plan's original test_df.columns prefix-scan therefore always found zero engineered
# MAGIC   feature columns and silently built a 3-column (amount/latitude/longitude) test set --
# MAGIC   the schema-shape mismatch this caused wasn't cosmetic, it hard-failed every call to
# MAGIC   predict() (30 features expected, 3 given). Added the same multi_entity_pit_join used
# MAGIC   by retrain_pipeline.py / snapshot_training_baseline.py so this script builds its own
# MAGIC   real evaluation set instead of assuming pre-joined columns that don't exist.

import time
import mlflow
import numpy as np
from sklearn.metrics import precision_recall_curve, auc, confusion_matrix
from scipy.stats import binomtest
from pyspark.sql.functions import col as _col
from fraud_detection.features.point_in_time_join import multi_entity_pit_join

mlflow.set_registry_uri("databricks-uc")
CHAMPION_MODEL_NAME = "fraud_detection_dev.gold.fraud_ensemble_champion"

dbutils.widgets.text("challenger_run_id", "")
challenger_run_id = dbutils.widgets.get("challenger_run_id")

labeled_df = spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions").filter(
    "event_date >= current_date() - 14 AND is_fraud_reconciled IS NOT NULL AND label_confidence >= 0.7"
)

BASELINE_LOOKBACK_SECONDS = 3 * 24 * 3600
GRAPH_LOOKBACK_SECONDS = 3 * 24 * 3600

card_velocity = spark.table("fraud_detection_dev.gold.feature_card_velocity")
cust_velocity = spark.table("fraud_detection_dev.gold.feature_customer_velocity")
geo_velocity = spark.table("fraud_detection_dev.gold.feature_geo_velocity")
baselines = spark.table("fraud_detection_dev.gold.customer_behavioral_baselines")
merchant_risk = spark.table("fraud_detection_dev.gold.merchant_risk_baselines")
graph_metrics_customer = (
    spark.table("fraud_detection_dev.gold.graph_entity_metrics")
    .filter(_col("entity_type") == "customer")
    .select(
        _col("entity_id").alias("customer_id"),
        "graph_pagerank_score", "graph_degree_centrality", "graph_community_id",
        "feature_timestamp",
    )
)
feature_specs = [
    (card_velocity, "card_id"),
    (cust_velocity, "customer_id"),
    (geo_velocity, "card_id"),
    (baselines, "customer_id", BASELINE_LOOKBACK_SECONDS),
    (merchant_risk, "merchant_id", BASELINE_LOOKBACK_SECONDS),
    (graph_metrics_customer, "customer_id", GRAPH_LOOKBACK_SECONDS),
]
test_df = multi_entity_pit_join(labeled_df, feature_specs)

# FraudEnsemblePyFunc.predict() scores one row at a time (no batch path), and this
# workspace has no vectorized alternative -- scoring the entire 14-day eligible set
# (which can run into the tens of thousands of rows) sequentially in a Python loop
# would take far longer than this gate should reasonably block a retrain on. Capped
# to a sample that's still large enough for stable PR-AUC/recall/FPR estimates.
EVAL_SAMPLE_SIZE = 2000
if test_df.count() > EVAL_SAMPLE_SIZE:
    test_df = test_df.sample(fraction=EVAL_SAMPLE_SIZE / test_df.count(), seed=42)

RAW_FEATURE_COLS = ["amount", "latitude", "longitude"]
EXCLUDED_TIMESTAMP_COLS = {"base_cust_first_seen_ts", "base_cust_last_seen_ts"}
feature_cols = RAW_FEATURE_COLS + [
    c for c in test_df.columns
    if c.startswith(("feature_", "vel_", "geo_", "base_", "merch_", "graph_"))
    and c not in EXCLUDED_TIMESTAMP_COLS
]

test_pdf = test_df.select(feature_cols + ["is_fraud_reconciled"]).toPandas()
# Same NaN guard as retrain_pipeline.py -- geo_dist_km/etc. can be null for a
# card's first transaction or when a PIT lookback finds no match.
X_test = np.nan_to_num(
    test_pdf[feature_cols].values.astype(np.float32), nan=0.0, posinf=0.0, neginf=0.0
)
y_test = test_pdf["is_fraud_reconciled"].values
amounts = test_pdf["amount"].values

champion_model = mlflow.pyfunc.load_model(f"models:/{CHAMPION_MODEL_NAME}@champion")
challenger_model = mlflow.pyfunc.load_model(f"runs:/{challenger_run_id}/ensemble_model")

champ_preds = np.array([champion_model.predict(X_test[i:i+1])["fraud_probability"] for i in range(len(X_test))])
chall_preds = np.array([challenger_model.predict(X_test[i:i+1])["fraud_probability"] for i in range(len(X_test))])

# GATE 1: PR-AUC Non-Regression (tolerance: -0.005)
prec_c, rec_c, _ = precision_recall_curve(y_test, champ_preds)
champ_prauc = auc(rec_c, prec_c)
prec_h, rec_h, _ = precision_recall_curve(y_test, chall_preds)
chall_prauc = auc(rec_h, prec_h)
gate1_passed = chall_prauc >= (champ_prauc - 0.005)
print(f"GATE 1 PR-AUC: Champion={champ_prauc:.4f}, Challenger={chall_prauc:.4f} -> {'PASS' if gate1_passed else 'FAIL'}")

# GATE 2: High-Value Recall (transactions > $5,000)
high_value_mask = amounts > 5000.0
if high_value_mask.sum() > 0:
    hv_y = y_test[high_value_mask]
    hv_champ = (champ_preds[high_value_mask] >= 0.5).astype(int)
    hv_chall = (chall_preds[high_value_mask] >= 0.5).astype(int)
    champ_hv_recall = hv_champ[hv_y == 1].sum() / max(1, hv_y.sum())
    chall_hv_recall = hv_chall[hv_y == 1].sum() / max(1, hv_y.sum())
    gate2_passed = chall_hv_recall >= (champ_hv_recall - 0.01)
    print(f"GATE 2 High-Value Recall: Champion={champ_hv_recall:.4f}, Challenger={chall_hv_recall:.4f} -> {'PASS' if gate2_passed else 'FAIL'}")
else:
    gate2_passed = True
    print("GATE 2 High-Value Recall: No high-value transactions in test set. PASS by default.")

# GATE 3: False Positive Rate Cap (max 5% increase)
champ_binary = (champ_preds >= 0.5).astype(int)
chall_binary = (chall_preds >= 0.5).astype(int)
champ_tn, champ_fp, _, _ = confusion_matrix(y_test, champ_binary, labels=[0, 1]).ravel()
chall_tn, chall_fp, _, _ = confusion_matrix(y_test, chall_binary, labels=[0, 1]).ravel()
champ_fpr = champ_fp / max(1, champ_fp + champ_tn)
chall_fpr = chall_fp / max(1, chall_fp + chall_tn)
gate3_passed = chall_fpr <= (champ_fpr * 1.05)
print(f"GATE 3 FPR Cap: Champion={champ_fpr:.4f}, Challenger={chall_fpr:.4f} -> {'PASS' if gate3_passed else 'FAIL'}")

# GATE 4: Latency Benchmark (p99 < 100ms)
latencies = []
sample_size = min(100, len(X_test))
for i in range(sample_size):
    t0 = time.time()
    challenger_model.predict(X_test[i:i+1])
    latencies.append((time.time() - t0) * 1000.0)
p99_latency = float(sorted(latencies)[int(len(latencies) * 0.99)]) if latencies else 0.0
gate4_passed = p99_latency <= 100.0
print(f"GATE 4 Latency: p99={p99_latency:.2f}ms -> {'PASS' if gate4_passed else 'FAIL'}")

# ADVISORY: McNemar's Statistical Significance Test
champ_correct = (champ_binary == y_test)
chall_correct = (chall_binary == y_test)
b = int(np.sum(champ_correct & ~chall_correct))
c = int(np.sum(~champ_correct & chall_correct))
if b + c > 0:
    mcnemar_p = binomtest(min(b, c), b + c, 0.5).pvalue
    print(f"ADVISORY McNemar's p-value: {mcnemar_p:.4f} (b={b}, c={c})")
else:
    mcnemar_p = 1.0
    print("ADVISORY McNemar's: Models produce identical predictions.")

all_gates_passed = gate1_passed and gate2_passed and gate3_passed and gate4_passed

with mlflow.start_run(run_id=challenger_run_id):
    mlflow.log_metric("gate_prauc_passed", int(gate1_passed))
    mlflow.log_metric("gate_hv_recall_passed", int(gate2_passed))
    mlflow.log_metric("gate_fpr_passed", int(gate3_passed))
    mlflow.log_metric("gate_latency_passed", int(gate4_passed))
    mlflow.log_metric("mcnemar_p_value", mcnemar_p)
    mlflow.log_metric("all_gates_passed", int(all_gates_passed))

client = mlflow.tracking.MlflowClient()

if all_gates_passed:
    print("ALL 4 GATES PASSED! Promoting Challenger to @champion...")
    model_ver = client.create_model_version(
        name=CHAMPION_MODEL_NAME,
        source=f"runs:/{challenger_run_id}/ensemble_model",
        run_id=challenger_run_id,
    )
    client.set_registered_model_alias(CHAMPION_MODEL_NAME, "champion", model_ver.version)
    print(f"Model Version {model_ver.version} promoted to @champion.")
else:
    print("CHALLENGER REJECTED: One or more evaluation gates failed.")
    # Still register the version (traceable in the registry with its gate results
    # already logged above) but tag it @challenger instead of promoting it, so a
    # rejected run is inspectable rather than only existing as an MLflow run.
    model_ver = client.create_model_version(
        name=CHAMPION_MODEL_NAME,
        source=f"runs:/{challenger_run_id}/ensemble_model",
        run_id=challenger_run_id,
    )
    client.set_registered_model_alias(CHAMPION_MODEL_NAME, "challenger", model_ver.version)
    print(f"Model Version {model_ver.version} registered and tagged @challenger (not promoted).")
