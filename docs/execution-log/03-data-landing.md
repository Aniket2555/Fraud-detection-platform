# 03 — Databricks Workspace Setup, Data Landing, Bronze→Silver→Gold, Baseline Model

**Starting state:** Databricks workspace existed (from Terraform) but had no
Unity Catalog objects, no cluster, no data, no notebooks executed.

**End state:** Unity Catalog fully configured; IEEE-CIS Kaggle dataset
landed in the Bronze layer; Silver/Gold transformed; XGBoost baseline
model trained and registered in MLflow/Unity Catalog with real metrics.

## Databricks workspace links

| Thing | Link |
|---|---|
| Workspace | https://adb-7405619338601349.9.azuredatabricks.net |
| Catalog explorer | https://adb-7405619338601349.9.azuredatabricks.net/explore/data/fraud_detection_dev |
| Baseline experiment | https://adb-7405619338601349.9.azuredatabricks.net/ml/experiments/3667937819264216 |
| Registered baseline model | https://adb-7405619338601349.9.azuredatabricks.net/explore/data/models/fraud_detection_dev/gold/fraud_xgboost_baseline |
| Compute / clusters page | https://adb-7405619338601349.9.azuredatabricks.net/compute |

## Databricks CLI setup

```bash
pip install databricks-cli
databricks configure --token
# Host: https://adb-7405619338601349.9.azuredatabricks.net
# Token: generated from the workspace's User Settings > Developer > Access tokens page (manual, browser)
```

## Unity Catalog: catalog, schemas, external locations

Run via `databricks/workspace-setup/` scripts, or manually:

```sql
CREATE CATALOG IF NOT EXISTS fraud_detection_dev;
CREATE SCHEMA IF NOT EXISTS fraud_detection_dev.bronze;
CREATE SCHEMA IF NOT EXISTS fraud_detection_dev.silver;
CREATE SCHEMA IF NOT EXISTS fraud_detection_dev.gold;
```

**Storage credential + external locations** (needed because the
workspace's auto-provisioned "default" storage credential is locked to
Databricks' own managed storage path — bug #10 in the overview list):

```sql
CREATE STORAGE CREDENTIAL cred_fraud_lake
  WITH (AZURE_MANAGED_IDENTITY = '<databricks-access-connector-resource-id>');

CREATE EXTERNAL LOCATION loc_bronze
  URL 'abfss://bronze@stfraudlakedev.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fraud_lake);
CREATE EXTERNAL LOCATION loc_silver
  URL 'abfss://silver@stfraudlakedev.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fraud_lake);
CREATE EXTERNAL LOCATION loc_checkpoints
  URL 'abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fraud_lake);
```

(The access connector's managed identity needs `Storage Blob Data
Contributor` on `stfraudlakedev` — granted via the `rbac-assignments`
Terraform module.)

## Cluster

Created once via the Databricks UI/API, single-node (this is a dev/free-
trial setup, not intended for multi-node parallelism):

```json
{
  "cluster_name": "batch-etl-dev",
  "spark_version": "14.3.x-scala2.12",
  "node_type_id": "Standard_D4s_v5",
  "num_workers": 0,
  "autotermination_minutes": 60,
  "single_user_name": "<your-email>",
  "data_security_mode": "SINGLE_USER"
}
```

### Starting/using the cluster manually

```bash
databricks clusters start 0830-043110-gk2nx3tn
databricks clusters get 0830-043110-gk2nx3tn --output json | grep '"state"'
# wait for RUNNING (3-7 minutes)
```

Or in the UI: Compute > `batch-etl-dev` > Start.

### Cluster libraries installed

- Maven: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`, `graphframes:graphframes:0.8.3-spark3.5-s_2.12`
- Wheel: this project's own `databricks/src` and `ml/` packages, built and
  uploaded via `databricks/workspace-setup/build_and_deploy_wheel.sh`
  (uses `pyproject.toml`'s `[tool.setuptools]` package-dir mapping)

Notebook-scoped installs (`%pip install ...` at the top of individual
notebooks) were used for anything not needed cluster-wide (e.g.
`nest_asyncio`, `gremlinpython`) to avoid bloating the base environment.

## Secret scope

```bash
# databricks/workspace-setup/secret_scope_setup.sh does this, but the
# actual KV name has a random suffix (bug #1/#2) so it resolves it first:
KV_NAME=$(az keyvault list --resource-group rg-fraud-detection-dev --query "[0].name" -o tsv)
databricks secrets create-scope kv-fraud --scope-backend-type AZURE_KEYVAULT \
  --resource-id "$(az keyvault show --name $KV_NAME --query id -o tsv)" \
  --dns-name "$(az keyvault show --name $KV_NAME --query properties.vaultUri -o tsv)"
```

## Landing the IEEE-CIS Kaggle dataset

```bash
pip install kaggle
export KAGGLE_USERNAME=<your kaggle username>
export KAGGLE_KEY=<your kaggle api key, from kaggle.com/settings>
kaggle competitions download -c ieee-fraud-detection -p /tmp/ieee-cis
unzip /tmp/ieee-cis/ieee-fraud-detection.zip -d /tmp/ieee-cis
```

Upload to ADLS (used `azcopy` for the larger files — faster and more
reliable than `az storage blob upload` for multi-hundred-MB files):

```bash
azcopy login   # interactive, browser-based, once
azcopy copy "/tmp/ieee-cis/train_transaction.csv" \
  "https://stfraudlakedev.dfs.core.windows.net/bronze/ieee-cis/train_transaction/train_transaction.csv"
azcopy copy "/tmp/ieee-cis/train_identity.csv" \
  "https://stfraudlakedev.dfs.core.windows.net/bronze/ieee-cis/train_identity/train_identity.csv"
```

## Running the Bronze → Silver → Gold notebooks

Executed via the Databricks Command Execution REST API (create an
execution context on the cluster, submit commands, poll for status) so
they could be run and monitored programmatically rather than by hand in
the UI. Manually, you'd just open each notebook and "Run All":

1. `databricks/notebooks/bronze/ingest_ieee_cis_transactions.py`
2. `databricks/notebooks/bronze/ingest_ieee_cis_identity.py`
3. `databricks/notebooks/silver/transform_ieee_cis_to_silver.py`
4. Gold aggregation notebook(s) under `databricks/notebooks/gold/`

Each uses Auto Loader (`cloudFiles` format) reading from the `bronze`
container with a schema location under `checkpoints`, so re-running is
idempotent (Auto Loader tracks already-processed files).

### Verifying it worked

```sql
SELECT COUNT(*) FROM fraud_detection_dev.bronze.ieee_cis_transactions;  -- ~590,540
SELECT COUNT(*) FROM fraud_detection_dev.silver.transactions;            -- 182 partitions/day-buckets after cleaning
SELECT COUNT(DISTINCT event_date) FROM fraud_detection_dev.silver.transactions;  -- should be >1 (bug #16 check)
```

## Baseline model training

```bash
# Command Execution API equivalent of running the notebook:
#   ml/training/train_xgboost_baseline.py
# (imports fraud_detection.ml.training.data_preparation, trains XGBoost
#  with Optuna-tuned hyperparameters, logs to MLflow, registers to UC)
```

Manually: attach `ml/training/train_xgboost_baseline.py` to the
`batch-etl-dev` cluster (or open it as a Databricks notebook) and Run All.

### Verifying it worked

Check the MLflow experiment UI link above — look for a run with logged
metrics (`pr_auc`, `roc_auc`, `precision`, `recall`) and a registered
model version under
`fraud_detection_dev.gold.fraud_xgboost_baseline`.

```bash
databricks registered-models get fraud_detection_dev.gold.fraud_xgboost_baseline
```

## To reproduce this from scratch

1. Start the cluster (`databricks clusters start <id>`).
2. Run the Unity Catalog SQL above (catalog/schemas/storage
   credential/external locations) — one time only.
3. Create the secret scope.
4. Download the IEEE-CIS dataset from Kaggle, upload the two CSVs to the
   `bronze` container via `azcopy`.
5. Run the bronze ingestion notebooks (Auto Loader will pick up the CSVs).
6. Run the silver transform notebook.
7. Run the baseline training notebook.
8. Check the MLflow experiment for results.
