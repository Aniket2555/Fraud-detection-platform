# Databricks notebook source
# MAGIC %md
# MAGIC # Champion vs. Challenger 4-Gate Evaluation with Statistical Significance

import time
import mlflow
import numpy as np
from sklearn.metrics import precision_recall_curve, auc, confusion_matrix
from scipy.stats import binom_test

dbutils.widgets.text("challenger_run_id", "")
challenger_run_id = dbutils.widgets.get("challenger_run_id")

test_df = spark.table("fraud_detection_dev.gold.reconciled_labeled_transactions").filter(
    "event_date >= current_date() - 14 AND is_fraud_reconciled IS NOT NULL AND label_confidence >= 0.7"
)
feature_cols = [c for c in test_df.columns if c.startswith(("feature_", "vel_", "geo_", "base_", "merch_", "graph_"))]

test_pdf = test_df.select(feature_cols + ["is_fraud_reconciled", "amount"]).toPandas()
X_test = test_pdf[feature_cols].values
y_test = test_pdf["is_fraud_reconciled"].values
amounts = test_pdf["amount"].values

champion_model = mlflow.pyfunc.load_model("models:/fraud-ensemble-champion/Production")
challenger_model = mlflow.pyfunc.load_model(f"runs:/{challenger_run_id}/ensemble_model")

champ_preds = np.array([champion_model.predict(X_test[i:i+1])["fraud_probability"] for i in range(len(X_test))])
chall_preds = np.array([challenger_model.predict(X_test[i:i+1])["fraud_probability"] for i in range(len(X_test))])

# GATE 1: PR-AUC Non-Regression (tolerance: -0.005)
prec_c, rec_c, _ = precision_recall_curve(y_test, champ_preds)
champ_prauc = auc(rec_c, prec_c)
prec_h, rec_h, _ = precision_recall_curve(y_test, chall_preds)
chall_prauc = auc(rec_h, prec_h)
gate1_passed = chall_prauc >= (champ_prauc - 0.005)
print(f"GATE 1 PR-AUC: Champion={champ_prauc:.4f}, Challenger={chall_prauc:.4f} -> {'✅ PASS' if gate1_passed else '❌ FAIL'}")

# GATE 2: High-Value Recall (transactions > $5,000)
high_value_mask = amounts > 5000.0
if high_value_mask.sum() > 0:
    hv_y = y_test[high_value_mask]
    hv_champ = (champ_preds[high_value_mask] >= 0.5).astype(int)
    hv_chall = (chall_preds[high_value_mask] >= 0.5).astype(int)
    champ_hv_recall = hv_champ[hv_y == 1].sum() / max(1, hv_y.sum())
    chall_hv_recall = hv_chall[hv_y == 1].sum() / max(1, hv_y.sum())
    gate2_passed = chall_hv_recall >= (champ_hv_recall - 0.01)
    print(f"GATE 2 High-Value Recall: Champion={champ_hv_recall:.4f}, Challenger={chall_hv_recall:.4f} -> {'✅ PASS' if gate2_passed else '❌ FAIL'}")
else:
    gate2_passed = True
    print("GATE 2 High-Value Recall: No high-value transactions in test set. PASS by default.")

# GATE 3: False Positive Rate Cap (max 5% increase)
champ_binary = (champ_preds >= 0.5).astype(int)
chall_binary = (chall_preds >= 0.5).astype(int)
champ_tn, champ_fp, _, _ = confusion_matrix(y_test, champ_binary).ravel()
chall_tn, chall_fp, _, _ = confusion_matrix(y_test, chall_binary).ravel()
champ_fpr = champ_fp / max(1, champ_fp + champ_tn)
chall_fpr = chall_fp / max(1, chall_fp + chall_tn)
gate3_passed = chall_fpr <= (champ_fpr * 1.05)
print(f"GATE 3 FPR Cap: Champion={champ_fpr:.4f}, Challenger={chall_fpr:.4f} -> {'✅ PASS' if gate3_passed else '❌ FAIL'}")

# GATE 4: Latency Benchmark (p99 < 100ms)
latencies = []
sample_size = min(100, len(X_test))
for i in range(sample_size):
    t0 = time.time()
    challenger_model.predict(X_test[i:i+1])
    latencies.append((time.time() - t0) * 1000.0)
p99_latency = float(sorted(latencies)[int(len(latencies) * 0.99)])
gate4_passed = p99_latency <= 100.0
print(f"GATE 4 Latency: p99={p99_latency:.2f}ms -> {'✅ PASS' if gate4_passed else '❌ FAIL'}")

# ADVISORY: McNemar's Statistical Significance Test
champ_correct = (champ_binary == y_test)
chall_correct = (chall_binary == y_test)
b = np.sum(champ_correct & ~chall_correct)
c = np.sum(~champ_correct & chall_correct)
if b + c > 0:
    mcnemar_p = binom_test(min(b, c), b + c, 0.5)
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

if all_gates_passed:
    print("✅ ALL 4 GATES PASSED! Promoting Challenger to Model Registry Production...")
    client = mlflow.tracking.MlflowClient()
    model_ver = client.create_model_version(
        name="fraud-ensemble-champion",
        source=f"runs:/{challenger_run_id}/ensemble_model",
        run_id=challenger_run_id
    )
    client.transition_model_version_stage(
        name="fraud-ensemble-champion",
        version=model_ver.version,
        stage="Production",
        archive_existing_versions=True
    )
    print(f"Model Version {model_ver.version} promoted to Production.")
else:
    print("❌ CHALLENGER REJECTED: One or more evaluation gates failed.")
