# End-to-End Operational Guide
## Real-Time Fraud Detection Platform

> **Scope:** Complete walkthrough from local prerequisites to live streaming inference with automated daily MLOps — every command, config value, and environment variable you need.

---

## Table of Contents

1. [Prerequisites & Local Setup](#1-prerequisites--local-setup)
2. [Phase 0 — Infrastructure Deployment](#2-phase-0--infrastructure-deployment)
3. [Phase 1 — Databricks Workspace Setup](#3-phase-1--databricks-workspace-setup)
4. [Phase 1 — Batch Data Ingestion & Baseline Model](#4-phase-1--batch-data-ingestion--baseline-model)
5. [Phase 2 — Streaming Pipeline](#5-phase-2--streaming-pipeline)
6. [Phase 3 — Feature Store](#6-phase-3--feature-store)
7. [Phase 4 — Hybrid ML Model Training & Serving](#7-phase-4--hybrid-ml-model-training--serving)
8. [Phase 5 — Decision Engine & Case Workflow Deployment](#8-phase-5--decision-engine--case-workflow-deployment)
9. [Phase 6 — MLOps Loop (Drift & Retraining)](#9-phase-6--mlops-loop-drift--retraining)
10. [Phase 7 — Governance, Security & Hardening](#10-phase-7--governance-security--hardening)
11. [End-to-End Smoke Test](#11-end-to-end-smoke-test)
12. [Troubleshooting Reference](#12-troubleshooting-reference)
13. [Quick Reference: Resource Names](#13-quick-reference-resource-names)

---

## 1. Prerequisites & Local Setup

### 1.1 Required Accounts & Subscriptions

| Requirement | Detail |
|---|---|
| **Azure Free Trial** | [azure.microsoft.com/free](https://azure.microsoft.com/en-us/free/) — $200 credit, 30 days |
| **Azure Subscription** | Verify with `az account list` — must show an active subscription |
| **GitHub Account** | To push the repo and enable CI/CD Actions |
| **Kaggle Account** | To download the IEEE-CIS Fraud Detection dataset |

### 1.2 Install Local Tooling

```powershell
# Azure CLI
winget install Microsoft.AzureCLI

# Databricks CLI
pip install databricks-cli

# Azure Functions Core Tools v4
winget install Microsoft.AzureFunctionsCoreTools

# Python 3.11 (project target runtime)
winget install Python.Python.3.11

# Git
winget install Git.Git
```

**Verify:**
```powershell
az --version          # Expect: 2.60+
databricks --version  # Expect: 0.18+
func --version        # Expect: 4.x
python --version      # Expect: 3.11.x
```

### 1.3 Clone the Repository

```powershell
git clone https://github.com/<your-org>/fraud-detection-platform.git
cd "fraud-detection-platform"
```

### 1.4 Login to Azure

```powershell
az login
az account set --subscription "YOUR-SUBSCRIPTION-ID"
az account show --query "{Name:name, ID:id, State:state}"
```

### 1.5 Download the Dataset

1. Go to [kaggle.com/c/ieee-fraud-detection/data](https://www.kaggle.com/c/ieee-fraud-detection/data)
2. Download `train_transaction.csv` and `train_identity.csv`
3. Place them in:

```
Project-2-Real-time-fraudlent detection/
└── data/
    ├── train_transaction.csv   (590,540 rows)
    └── train_identity.csv      (144,233 rows)
```

> **Note:** `data/` is in `.gitignore`. Dataset files never get committed.

---

## 2. Phase 0 — Infrastructure Deployment

### 2.1 Configure Deployment Parameters

Edit `infrastructure/modules/parameters/dev.parameters.json`:

```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#",
  "contentVersion": "1.0.0.0",
  "parameters": {
    "environment":      { "value": "dev" },
    "location":         { "value": "centralindia" },
    "ownerEmail":       { "value": "YOUR-EMAIL@example.com" },
    "tenantId":         { "value": "YOUR-TENANT-ID" },
    "deployerObjectId": { "value": "YOUR-OBJECT-ID" }
  }
}
```

**Get your IDs:**
```powershell
# Tenant ID
az account show --query tenantId -o tsv

# Your AAD Object ID (the deployer — you)
az ad signed-in-user show --query id -o tsv
```

### 2.2 Deploy All Azure Resources via Bicep

```powershell
az deployment sub create `
  --name "fraud-detection-$(Get-Date -Format 'yyyyMMddHHmm')" `
  --location centralindia `
  --template-file infrastructure/main.bicep `
  --parameters infrastructure/modules/parameters/dev.parameters.json `
  --verbose
```

**Expected time:** ~12–18 minutes.

**Resources provisioned:**

| Resource | Name | Purpose |
|---|---|---|
| Resource Group | `rg-fraud-detection-dev` | Container for all resources |
| Key Vault | `kv-fraud-dev` | Secrets & PII salt |
| ADLS Gen2 | `stfraudlakedev` | Delta Lake (8 containers) |
| Databricks Workspace | `dbw-fraud-dev` | PySpark, MLflow |
| Event Hubs Namespace | `ehns-fraud-dev` | Streaming ingestion (4 partitions) |
| Azure SQL Serverless | `sql-fraud-dev` | Case management DB |
| Service Bus | `sbns-fraud-dev` | Decision fan-out (3 subscriptions) |
| App Configuration | `appcs-fraud-dev` | Live threshold management |
| Cosmos DB Gremlin | `cosmos-fraud-dev` | Graph features |
| Log Analytics | `log-fraud-dev` | Centralised monitoring |

### 2.3 Populate Key Vault Secrets

```powershell
$KV = "kv-fraud-dev"

# Event Hubs connection string
# Azure Portal → Event Hubs Namespace → Shared Access Policies → Copy Connection String
az keyvault secret set --vault-name $KV --name "eventhub-conn-str" `
  --value "Endpoint=sb://ehns-fraud-dev.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."

# Service Bus connection string
az keyvault secret set --vault-name $KV --name "servicebus-conn-str" `
  --value "Endpoint=sb://sbns-fraud-dev.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."

# Azure SQL admin password (same as set during Bicep deploy)
az keyvault secret set --vault-name $KV --name "sql-admin-password-dev" --value "YOUR-STRONG-PASSWORD"

# PII hashing salt (generate 32 random chars)
$SALT = -join ((65..90)+(97..122)+(48..57) | Get-Random -Count 32 | % {[char]$_})
az keyvault secret set --vault-name $KV --name "pii-hash-salt" --value $SALT

# Storage access key (for Databricks cluster config during Free Trial)
$STORAGE_KEY = az storage account keys list --account-name stfraudlakedev --query '[0].value' -o tsv
az keyvault secret set --vault-name $KV --name "storage-access-key" --value $STORAGE_KEY
```

### 2.4 Run Phase 0 Smoke Test

```bash
bash infrastructure/scripts/smoke_test.sh dev
```

**Expected output:**
```
==========================================
Phase 0 Smoke Test — Environment: dev
==========================================
✅ PASS: Resource group exists
✅ PASS: Key Vault exists
✅ PASS: Key Vault soft-delete enabled
✅ PASS: Placeholder secrets exist
✅ PASS: Storage account exists
✅ PASS: Hierarchical namespace enabled
✅ PASS: Container 'bronze' exists
✅ PASS: Databricks workspace exists
==========================================
Results: 8 passed, 0 failed
✅ Phase 0 smoke test PASSED
```

---

## 3. Phase 1 — Databricks Workspace Setup

### 3.1 Configure Databricks CLI

```powershell
# Get workspace URL: Azure Portal → Databricks Workspace → Overview → URL
databricks configure --token
# Prompt 1: Databricks Host → https://adb-XXXXXXXXXX.XX.azuredatabricks.net
# Prompt 2: Token → Databricks UI → User Settings → Developer → Access Tokens → Generate New Token
```

### 3.2 Create Unity Catalog Schemas

Open **Databricks UI → SQL Editor** and run:

```sql
-- From: databricks/workspace-setup/create_catalog_schemas.sql
CREATE CATALOG IF NOT EXISTS fraud_detection_dev
  COMMENT 'Fraud Detection Platform — Development Environment';

USE CATALOG fraud_detection_dev;

CREATE SCHEMA IF NOT EXISTS bronze    COMMENT 'Raw, append-only data';
CREATE SCHEMA IF NOT EXISTS silver    COMMENT 'Cleaned, deduplicated, quality-gated data';
CREATE SCHEMA IF NOT EXISTS gold      COMMENT 'Business-ready aggregations and feature store';
CREATE SCHEMA IF NOT EXISTS quarantine COMMENT 'Rejected rows from quality gates';
CREATE SCHEMA IF NOT EXISTS reference COMMENT 'Merchant categories, FX rates, risk tiers';

SHOW SCHEMAS IN fraud_detection_dev;
```

> **Important:** Unity Catalog requires the 14-day Premium trial that begins on workspace creation day. Run this SQL within the first 14 days. After trial expiry, use the Hive Metastore fallback commands at the bottom of `create_catalog_schemas.sql`.

### 3.3 Link Databricks Secret Scope to Key Vault

```bash
bash databricks/workspace-setup/secret_scope_setup.sh dev
```

**Verify:**
```bash
databricks secrets list-scopes
databricks secrets list --scope kv-fraud
# Expected secrets: eventhub-conn-str, servicebus-conn-str, pii-hash-salt, sql-admin-password-dev, storage-access-key
```

### 3.4 Configure ADLS Gen2 Access on the Cluster

When creating a Databricks cluster, add these **Spark Config** properties (under Advanced Options):

```
spark.hadoop.fs.azure.account.key.stfraudlakedev.dfs.core.windows.net  {{secrets/kv-fraud/storage-access-key}}
```

> **Free Trial note:** This uses the Storage Account Access Key from Key Vault — the cheapest auth method requiring no Service Principal setup.

---

## 4. Phase 1 — Batch Data Ingestion & Baseline Model

### 4.1 Upload Dataset to ADLS Gen2 Raw Container

```powershell
$STORAGE = "stfraudlakedev"

az storage fs file upload `
  --source "data/train_transaction.csv" `
  --file-system "raw" --path "ieee-cis/train_transaction.csv" `
  --account-name $STORAGE --auth-mode login

az storage fs file upload `
  --source "data/train_identity.csv" `
  --file-system "raw" --path "ieee-cis/train_identity.csv" `
  --account-name $STORAGE --auth-mode login
```

### 4.2 Run Medallion Bronze → Silver → Gold Notebooks

In Databricks Workspace, open and run these **in order** on a `Standard_DS3_v2` single-node cluster:

| Order | Notebook | Output | Runtime |
|---|---|---|---|
| 1 | `databricks/notebooks/bronze/batch_ingest_ieee_cis.py` | `bronze.transactions` | ~5 min |
| 2 | `databricks/notebooks/quality/data_quality_gates.py` | `quarantine.rejected_transactions` | ~3 min |
| 3 | `databricks/notebooks/silver/silver_transformation.py` | `silver.transactions` (PII hashed) | ~8 min |
| 4 | `databricks/notebooks/gold/gold_aggregation.py` | `gold.transaction_aggregates` | ~5 min |

**Validate Silver row count:**
```sql
-- Databricks SQL Editor
SELECT COUNT(*) FROM fraud_detection_dev.silver.transactions;
-- Expected: ~550,000+ (some rows quarantined by quality gates)
```

### 4.3 Train the XGBoost Baseline Model

```powershell
python ml/training/train_xgboost_baseline.py
```

**Expected output:**
```
Baseline XGBoost Training Complete.
  Validation PR-AUC : 0.847
  Validation ROC-AUC: 0.931
  Model saved: models/baseline/xgb_baseline.pkl
```

> PR-AUC ≥ 0.80 is the quality gate. If below, check your data split and class weighting.

### 4.4 Run the Full Ensemble Training Pipeline

```powershell
# Runs all 9 steps: data load → quality gate → temporal split → SMOTE → XGBoost (Optuna) →
# Autoencoder → Isolation Forest → Calibration → Meta-Learner → MLflow registration
python ml/pipelines/training_pipeline.py
```

---

## 5. Phase 2 — Streaming Pipeline

### 5.1 Verify Event Hub Partition Configuration

```powershell
az eventhubs eventhub show `
  --resource-group rg-fraud-detection-dev `
  --namespace-name ehns-fraud-dev `
  --name eh-transactions `
  --query "{Partitions:partitionCount, Status:status}"
# Expected: {"Partitions": 4, "Status": "Active"}
```

### 5.2 Configure & Start the Transaction Producer

```powershell
cd "producers/transaction_producer"
pip install -r requirements.txt
```

Create `producers/transaction_producer/.env`:
```env
EVENTHUB_PRODUCER_CONN_STR=Endpoint=sb://ehns-fraud-dev.servicebus.windows.net/;SharedAccessKeyName=eh-producer-policy;SharedAccessKey=YOUR_KEY
EVENTHUB_NAME=eh-transactions
DATASET_PATH=../../data/train_transaction.csv
SPEED_MULTIPLIER=100
BATCH_SIZE=100
MAX_EVENTS=10000
LOG_INTERVAL=1000
VALIDATE_SCHEMA=true
```

```powershell
python producer.py
```

**Expected output:**
```
[Producer] Starting IEEE-CIS replay at 100x speed
[Producer] Batch 1 sent: 100 events (txn_000001 → txn_000100)
[Producer] Batch 2 sent: 100 events (txn_000101 → txn_000200)
...
[Producer] 10,000 events sent. Throughput: ~2,800 events/sec
```

### 5.3 Start the Structured Streaming Consumer

In Databricks, create a **Job Cluster** (not interactive) with `autotermination_minutes: 0`, then run:

`databricks/notebooks/bronze/streaming_consumer.py`

This runs continuously, reading from Event Hubs and applying `MERGE INTO bronze.raw_events` with checkpoint recovery on restart.

---

## 6. Phase 3 — Feature Store

Run all feature engineering notebooks **in order**:

| Order | Notebook | Output Table | Runtime |
|---|---|---|---|
| 1 | `notebooks/features/compute_card_velocity_features.py` | `gold.feature_card_velocity` | ~10 min |
| 2 | `notebooks/features/compute_customer_velocity_features.py` | `gold.feature_customer_velocity` | ~8 min |
| 3 | `notebooks/features/compute_geo_velocity_features.py` | `gold.feature_geo_velocity` | ~12 min |
| 4 | `notebooks/features/compute_graph_features.py` | `gold.feature_graph_network` | ~15 min |
| 5 | `notebooks/features/build_baseline_features.py` | `gold.train_feature_snapshot` | ~5 min |

**Validate:**
```sql
SELECT COUNT(*) FROM fraud_detection_dev.gold.train_feature_snapshot;
-- Expected: ~500,000+ rows with 43 feature columns
```

---

## 7. Phase 4 — Hybrid ML Model Training & Serving

### 7.1 Run Full Ensemble Training Pipeline

In Databricks, run `ml/pipelines/training_pipeline.py`.

**Trains in sequence:** XGBoost (Optuna 30 trials) → PyTorch Autoencoder (30 epochs) → Isolation Forest → 3× Isotonic Calibrators → Stacking Meta-Learner.

**MLflow Experiment:** `/Shared/fraud_detection_training`

The pipeline raises `ValueError` and aborts if test PR-AUC < 0.80 (quality gate).

### 7.2 Register the Champion Model in MLflow

After training, in a Databricks notebook:

```python
import mlflow
client = mlflow.tracking.MlflowClient()

# Find the run ID in: Experiments → fraud_detection_training → best test_pr_auc run
RUN_ID = "PASTE-BEST-RUN-ID-FROM-MLFLOW-UI"

model_ver = client.create_model_version(
    name="fraud-ensemble-champion",
    source=f"runs:/{RUN_ID}/ensemble_model",
    run_id=RUN_ID
)
client.transition_model_version_stage(
    name="fraud-ensemble-champion",
    version=model_ver.version,
    stage="Production",
    archive_existing_versions=True
)
print(f"Champion registered: version={model_ver.version}, stage=Production")
```

### 7.3 Deploy Azure ML Online Endpoint

```powershell
az ml online-endpoint create `
  --name "fraud-scoring-endpoint" `
  --resource-group rg-fraud-detection-dev `
  --workspace-name "mlw-fraud-dev"

az ml online-deployment create `
  --name "blue" `
  --endpoint-name "fraud-scoring-endpoint" `
  --file ml/serving/deployment_spec.yaml
```

**Test the scoring endpoint:**
```powershell
az ml online-endpoint invoke `
  --name "fraud-scoring-endpoint" `
  --request-file ml/tests/sample_request.json
```

**Expected response:**
```json
{
  "transaction_id": "txn_sample_001",
  "fraud_probability": 0.0423,
  "component_scores": {"xgboost": 0.038, "autoencoder": 0.041, "isolation_forest": 0.052},
  "top_risk_factors": [],
  "scoring_mode": "full",
  "model_version": "a3b2c1d4",
  "latency_ms": 18.4
}
```

---

## 8. Phase 5 — Decision Engine & Case Workflow Deployment

### 8.1 Deploy Azure SQL Schema

```powershell
$SQL = "sql-fraud-dev.database.windows.net"
$DB  = "sqldb-fraud-cases-dev"
$USR = "sqladmin"

# Run all 5 migrations in order
@("V001","V002","V003","V004","V005") | ForEach-Object {
    $file = Get-Item "database/migrations/${_}__*.sql"
    sqlcmd -S $SQL -d $DB -U $USR -P "YOUR-PASSWORD" -i $file.FullName
    Write-Host "✅ Migration $_ applied"
}
```

### 8.2 Set Decision Thresholds in App Configuration

```powershell
$AC = "appcs-fraud-dev"
az appconfig kv set --name $AC --key "FraudEngine:ApproveMaxThreshold" --value "0.10" --yes
az appconfig kv set --name $AC --key "FraudEngine:StepUpMaxThreshold"  --value "0.60" --yes
az appconfig kv set --name $AC --key "FraudEngine:BlockMinThreshold"   --value "0.90" --yes

# Verify
az appconfig kv list --name $AC --output table
```

### 8.3 Configure Function App Settings

```powershell
$FUNC = "func-decision-engine-dev"
$RG   = "rg-fraud-detection-dev"
$AC_CONN = az appconfig credential list --name appcs-fraud-dev --query '[0].connectionString' -o tsv

az functionapp config appsettings set `
  --name $FUNC --resource-group $RG `
  --settings `
    "SERVICE_BUS_CONN_STR=@Microsoft.KeyVault(VaultName=kv-fraud-dev;SecretName=servicebus-conn-str)" `
    "APP_CONFIG_CONN_STR=$AC_CONN" `
    "SERVICE_BUS_TOPIC_NAME=sb-topic-fraud-events" `
    "ADLS_STORAGE_ACCOUNT_NAME=stfraudlakedev" `
    "FRAUD_ENV=dev"
```

### 8.4 Deploy the Functions

```powershell
# Decision Engine
cd functions/decision_engine
func azure functionapp publish func-decision-engine-dev --python

# Audit Logger (same Function App, different trigger)
cd ../audit_logger
func azure functionapp publish func-decision-engine-dev --python
```

### 8.5 Test the Decision Engine

```powershell
$FUNC_KEY = az functionapp keys list `
  --name func-decision-engine-dev `
  --resource-group rg-fraud-detection-dev `
  --query "functionKeys.default" -o tsv

$URL = "https://func-decision-engine-dev.azurewebsites.net/api/evaluate-decision?code=$FUNC_KEY"

# Low-risk → approve
Invoke-RestMethod -Uri $URL -Method POST -ContentType "application/json" -Body '{
  "transaction_id": "test-low-001",
  "customer_id": "cust-abc",
  "card_id": "card-xyz",
  "amount": 25.00,
  "fraud_probability": 0.04,
  "model_version": "test"
}'
# Expected: {"decision_action": "approve", "fraud_probability": 0.04}

# High-risk → block
Invoke-RestMethod -Uri $URL -Method POST -ContentType "application/json" -Body '{
  "transaction_id": "test-high-001",
  "customer_id": "cust-abc",
  "card_id": "card-xyz",
  "amount": 9800.00,
  "fraud_probability": 0.95,
  "model_version": "test"
}'
# Expected: {"decision_action": "block", "fraud_probability": 0.95}
```

---

## 9. Phase 6 — MLOps Loop (Drift & Retraining)

### 9.1 Create the Daily MLOps Databricks Job

```powershell
databricks jobs create --json-file databricks/jobs/mlops_drift_and_retrain_job.json

# Verify
databricks jobs list --output table
# Expected: mlops_daily_drift_and_retrain — Schedule: 0 0 6 * * ? (06:00 IST)
```

This creates a 4-task sequential daily job:

| Task | Notebook | Purpose |
|---|---|---|
| 1 | `ingest_chargeback_feedback` | MERGE-based label reconciliation (30-day maturation) |
| 2 | `track_model_kpis` | Daily TP/FP/FN/TN + precision/recall/FPR aggregation |
| 3 | `run_daily_drift_check` | PSI/KS/JSD evaluation → auto-triggers retraining on critical drift |
| 4 | `shadow_scoring_batch` | Champion vs Challenger side-by-side scoring |

### 9.2 Trigger a Manual First Run

```powershell
$JOB_ID = databricks jobs list --output json | ConvertFrom-Json | `
  Where-Object { $_.settings.name -eq "mlops_daily_drift_and_retrain" } | `
  Select-Object -ExpandProperty job_id

databricks runs submit --job-id $JOB_ID
databricks runs get --run-id $(databricks runs list --limit 1 --output json | ConvertFrom-Json).runs[0].run_id
```

### 9.3 Monitor Drift History

```sql
-- Databricks SQL Editor
SELECT check_date, feature_drift_status, max_psi, mean_jsd,
       critical_features, warning_features, concept_drift_status
FROM fraud_detection_dev.gold.drift_monitoring_history
ORDER BY check_date DESC
LIMIT 10;
```

### 9.4 Monitor KPI Trends

```sql
SELECT kpi_date, model_version,
       true_positives, false_positives, false_negatives, true_negatives,
       ROUND(precision, 4) AS precision,
       ROUND(recall, 4) AS recall,
       ROUND(false_positive_rate, 4) AS fpr
FROM fraud_detection_dev.gold.model_performance_kpis
ORDER BY kpi_date DESC
LIMIT 14;
```

### 9.5 Deploy Rollback Sentinel

Add this as a recurring Databricks Job (every 6 hours) using a single-node cluster:

```python
# Job Notebook: scripts/automated_rollback_sentinel.py
# Schedule: 0 0 */6 * * ? (every 6 hours)
```

---

## 10. Phase 7 — Governance, Security & Hardening

### 10.1 Apply Unity Catalog Column Masking

In Databricks SQL Editor, run the entire script:
`databricks/governance/apply_data_masking_policies.sql`

Key operations:
- Creates `mask_ip_address()`, `mask_device_id()`, `mask_email()` masking functions
- Applies column-level masks to `silver.transactions`
- Grants role-based access to `fraud-analysts`, `data-engineers`, `ml-engineers`

**Verify masking works:**
```sql
-- Connect as a user in the 'fraud-analysts' group — should see masked IPs
SELECT ip_address, device_id FROM fraud_detection_dev.silver.transactions LIMIT 5;
-- Expected: "192.168.xxx.xxx" and "a1b2****" (not raw values)
```

### 10.2 Create Entra ID Groups

```powershell
az ad group create --display-name "fraud-analysts"       --mail-nickname "fraud-analysts"
az ad group create --display-name "data-engineers"       --mail-nickname "data-engineers"
az ad group create --display-name "ml-engineers"         --mail-nickname "ml-engineers"
az ad group create --display-name "compliance-officers"  --mail-nickname "compliance-officers"
az ad group create --display-name "platform-admins"      --mail-nickname "platform-admins"
```

### 10.3 Deploy RBAC Bicep Module

```powershell
# Get the Managed Identity Object IDs of your Function App and Databricks workspace
$FUNC_MI = az functionapp identity show `
  --name func-decision-engine-dev --resource-group rg-fraud-detection-dev `
  --query principalId -o tsv

$DBW_MI = az databricks workspace show `
  --name dbw-fraud-dev --resource-group rg-fraud-detection-dev `
  --query identity.principalId -o tsv

az deployment group create `
  --resource-group rg-fraud-detection-dev `
  --template-file infrastructure/modules/rbac-assignments.bicep `
  --parameters `
    environment=dev `
    storageAccountName=stfraudlakedev `
    keyVaultName=kv-fraud-dev `
    databricksPrincipalId=$DBW_MI `
    decisionFunctionPrincipalId=$FUNC_MI
```

### 10.4 Enable CI/CD Security Scanning

Push the repository to GitHub to activate the security scan workflow on every PR:

```powershell
git remote add origin https://github.com/<your-org>/fraud-detection-platform.git
git push -u origin main
```

Every pull request now auto-runs `.github/workflows/security-scan.yml`:
- **TruffleHog** — Verified secret leak detection
- **Checkov** — Bicep IaC misconfiguration scanning
- **Bandit** — Python SAST (hardcoded credentials, insecure patterns)
- **pip-audit** — Dependency CVE scanning

### 10.5 Rotate Key Vault Secrets

```powershell
$env:KEY_VAULT_NAME = "kv-fraud-dev"
python scripts/rotate_keyvault_secrets.py
# Rotates: db-admin-password-dev, pii-hash-salt, sql-admin-password-dev
```

### 10.6 Run the Full Platform Verification

```bash
bash scripts/verify_platform_end_to_end.sh
```

---

## 11. End-to-End Smoke Test

Run this sequence to validate every layer is connected:

```powershell
# 1. Send 100 streaming transactions
cd producers/transaction_producer
$env:MAX_EVENTS = "100"; python producer.py

# 2. Verify they landed in Bronze Delta
#    Databricks SQL: SELECT COUNT(*) FROM fraud_detection_dev.bronze.raw_events
#    WHERE event_date = current_date()

# 3. Test low-risk → approve
$URL = "https://func-decision-engine-dev.azurewebsites.net/api/evaluate-decision?code=$FUNC_KEY"
Invoke-RestMethod -Uri $URL -Method POST -ContentType "application/json" -Body `
  '{"transaction_id":"smoke-001","customer_id":"c1","card_id":"k1","amount":25.00,"fraud_probability":0.04,"model_version":"test"}'

# 4. Test high-risk → block
Invoke-RestMethod -Uri $URL -Method POST -ContentType "application/json" -Body `
  '{"transaction_id":"smoke-002","customer_id":"c1","card_id":"k1","amount":9999.00,"fraud_probability":0.95,"model_version":"test"}'

# 5. Verify audit record in ADLS Gold container
az storage fs file list `
  --file-system gold --path "audit_logs" `
  --account-name stfraudlakedev --auth-mode login

# 6. Verify Service Bus received the event
az servicebus topic show `
  --resource-group rg-fraud-detection-dev `
  --namespace-name sbns-fraud-dev `
  --name sb-topic-fraud-events `
  --query "countDetails"

# 7. Verify fraud case in SQL
#    sqlcmd -S sql-fraud-dev.database.windows.net -d sqldb-fraud-cases-dev -U sqladmin
#    SELECT TOP 5 transaction_id, decision_action, case_status, created_at
#    FROM fraud_cases ORDER BY created_at DESC;
```

---

## 12. Troubleshooting Reference

| Symptom | Likely Cause | Fix |
|---|---|---|
| `az deployment sub create` fails with "QuotaExceeded" | Free Trial vCPU limits | Use `Standard_DS2_v2` (2 vCPU) instead of DS3_v2 in Bicep params |
| Databricks cluster fails to start | vCPU quota exhausted | Use Single Node cluster: `num_workers: 0`, `cluster.profile: singleNode` |
| `pii_masking.py` raises `RuntimeError` on secret fetch | Key Vault unreachable | Set `FRAUD_ENV=dev` in Databricks cluster environment variables |
| Decision Engine returns `approve_fallback` | Required fields missing from request body | Ensure payload includes: `transaction_id`, `customer_id`, `card_id`, `amount`, `fraud_probability` |
| `track_model_performance_kpis.py` produces 0 rows | No reconciled labels yet | Run `ingest_chargeback_feedback.py` first to seed `gold.reconciled_labeled_transactions` |
| MLflow shows runs in `RUNNING` state indefinitely | Optuna nested runs not closed | Ensure `train_supervised.py` uses `with mlflow.start_run(nested=True):` context manager |
| `score.py` logs `model_version: "unknown"` | MLflow model not tagged | Re-register the model ensuring `run_id` is populated in MLflow model version metadata |
| Service Bus messages not reaching `sub-audit-log` | Subscription filter misconfigured | Azure Portal → Service Bus → Topic → Subscriptions → sub-audit-log → Filters |
| Shadow scoring exits `NO_CHALLENGER` | No model in Staging stage | Trigger a retraining run — champion_challenger_gate.py promotes to Staging on pass |
| `drift_detector.py` returns `NO_DATA` for all features | Feature snapshot table empty | Run Phase 3 feature notebooks to populate `gold.train_feature_snapshot` |
| CI fails TruffleHog scan | Credentials committed to git history | `git log --all --full-history -- "**/*.json"` to find commit; use `git filter-repo` to purge |
| Autoencoder reconstruction error is all zeros | Autoencoder not trained | Check `ae_model.pt` artifact exists and was trained on legitimate transactions only |
| KPI table accumulates duplicate rows per day | Using `.mode("append")` | Ensure production `track_model_performance_kpis.py` (with MERGE INTO fix) is deployed |

---

## 13. Quick Reference: Resource Names

| Category | Resource | Name |
|---|---|---|
| Azure | Resource Group | `rg-fraud-detection-dev` |
| Azure | Key Vault | `kv-fraud-dev` |
| Azure | ADLS Gen2 | `stfraudlakedev` |
| Azure | Databricks Workspace | `dbw-fraud-dev` |
| Azure | Event Hubs Namespace | `ehns-fraud-dev` |
| Azure | Event Hub | `eh-transactions` |
| Azure | Azure SQL Server | `sql-fraud-dev` |
| Azure | SQL Database | `sqldb-fraud-cases-dev` |
| Azure | Service Bus Namespace | `sbns-fraud-dev` |
| Azure | Service Bus Topic | `sb-topic-fraud-events` |
| Azure | App Configuration | `appcs-fraud-dev` |
| Azure | Function App | `func-decision-engine-dev` |
| Databricks | Unity Catalog | `fraud_detection_dev` |
| Databricks | Secret Scope | `kv-fraud` |
| MLflow | Registered Model | `fraud-ensemble-champion` |
| MLflow | Experiment | `/Shared/fraud_detection_training` |
| Key Vault Secrets | Event Hub conn | `eventhub-conn-str` |
| Key Vault Secrets | Service Bus conn | `servicebus-conn-str` |
| Key Vault Secrets | PII salt | `pii-hash-salt` |
| Key Vault Secrets | SQL password | `sql-admin-password-dev` |
| Key Vault Secrets | Storage key | `storage-access-key` |
| Environment Variables | PII env flag | `FRAUD_ENV=dev` |
