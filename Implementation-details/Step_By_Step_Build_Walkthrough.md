# Step-by-Step Build Walkthrough — Real-Time Fraud Detection Platform

This is a distilled, phase-by-phase learning document: for each phase, **why**
things were designed the way they were (decisions + tradeoffs), **how** they
were built (commands actually run), and **what broke** when the code was
actually executed against real Azure infrastructure for the first time (root
cause + fix, not just "fixed a bug").

Source material this was compiled from:
- `Implementation-details/Phase_0..7_implementation_plan.md` — "Production
  Decision Registry" tables (the *why*) and "Known Issues & Fixes Applied"
  sections (bugs caught by code review, before anything was ever run)
- `docs/execution-log/00..10-*.md` — the first real deployment/execution
  pass (the *what broke when it actually ran* — commands, root causes, real
  verification numbers)

**Read this top-to-bottom for the story, or jump to a phase.** Every phase
follows the same shape: Decisions → Commands → Bugs (code-review-time, then
runtime) → Verification.

---

## Environment quick-reference

| Thing | Value |
|---|---|
| Azure subscription | `800df714-bea7-4580-8606-22b36ebee0fa` ("Azure subscription 1") |
| Resource group | `rg-fraud-detection-dev` |
| Storage account (data lake) | `stfraudlakedev` |
| Key Vault | `kv-fraud-dev-4th9` (random suffix — see Phase 0) |
| Databricks workspace | `adb-7405619338601349.9.azuredatabricks.net` |
| Databricks cluster | `batch-etl-dev` (`Standard_D4s_v5`, single-node) |
| Unity Catalog catalog | `fraud_detection_dev` |
| Cosmos DB account | `cosmos-fraud-dev-604t` |
| Event Hubs namespace / hub | `ehns-fraud-dev` / `eh-transactions` |
| Azure SQL server | `sql-fraud-dev.database.windows.net` |
| App Configuration | `appcs-fraud-dev` |
| GitHub repo | `Aniket2555/Fraud-detection-platform` |

**Budget context that shapes almost every decision below:** this ran on a
$200/30-day Azure Free Trial. Tiered budget alerts (25%/50%/80% of spend)
were set on day 1, and the 80% alert was paired with an instruction to
immediately terminate every Databricks cluster.

---

## Phase 0 — Environment & Infrastructure Foundation

**Goal:** local dev environment working, all Azure infra live via Terraform, CI/CD secrets configured.

### Key decisions (why it looks the way it does)

| Decision | Full-production would do | What was built instead | Why |
|---|---|---|---|
| Environments | dev/staging/prod, 3 resource groups | **1 resource group**, `rg-fraud-detection-dev` | 3 environments = 3x the burn rate on a fixed $200 |
| Networking | VNet injection, 6 subnets, private endpoints, 9 Private DNS zones | **Default managed VNet + service firewalls** | Private endpoints cost ~$7.20/endpoint/month; VNet injection needs Premium tier |
| ML platform | Azure ML Managed Online Endpoints | **MLflow on Databricks only** | Azure ML endpoint = continuous compute cost — deferred entirely |
| Databricks | Premium, ongoing | Premium **for the 14-day trial window only** | Premium needed for Unity Catalog, but not free long-term |
| Cluster policy | long-lived clusters | **auto-terminate after 20 min idle**, never left overnight | A 2-worker cluster running 8h/day ≈ $15-25/day — could exhaust the budget in under 2 weeks |

### Commands run

```powershell
# Tools
winget install -e --id Microsoft.AzureCLI
winget install -e --id GitHub.cli
az login          # interactive, browser
gh auth login      # interactive, browser
```

```bash
# Terraform: bootstrap remote state (one-time)
cd infrastructure/bootstrap
terraform init
terraform plan -var="environment=dev" -out=bootstrap.tfplan
terraform apply bootstrap.tfplan
# -> storage_account_name: sttfstatedevmvm94i

# Terraform: deploy everything
cd infrastructure
export TF_VAR_sql_admin_password="$(cat infrastructure/.sql_admin_password.local)"
terraform init -input=false -backend-config=backend-dev.conf
terraform plan -input=false -var-file=environments/dev.tfvars -out=dev.tfplan
terraform apply -input=false dev.tfplan
# -> 64 resources live; ~$20-25/month base cost (Event Hubs + Service Bus Standard)
```

```bash
# CI service principal + GitHub Actions secrets (11 total)
az ad sp create-for-rbac --name "sp-fraud-detection-dev-ci" \
  --role Contributor --scopes "/subscriptions/<sub>/resourceGroups/rg-fraud-detection-dev"
gh secret set AZURE_SUBSCRIPTION_ID --repo Aniket2555/Fraud-detection-platform --body "<sub>"
# ... (10 more secrets: TENANT_ID, CLIENT_ID/SECRET, TFSTATE_*, OWNER_EMAIL,
#      DEPLOYER_OBJECT_ID, SQL_ADMIN_PASSWORD, DECISION_FUNCTION_PRINCIPAL_ID,
#      LOGIC_APP_PRINCIPAL_ID — the last two left empty until Phase 5)
```

**Windows/Git-Bash gotcha hit repeatedly throughout the whole project:** paths
starting with `/` (e.g. `/subscriptions/...`) get auto-mangled into Windows
paths unless prefixed with `MSYS_NO_PATHCONV=1`; `PATH` entries must use the
Unix mount form (`/c/Program Files/...`), not `C:/...` (the `:` collides with
the drive letter's own colon).

### Bugs found — local environment (before `pytest` could even run)

Starting state: 5 passing / 12 failing, `pytest` couldn't collect tests.

1. **Missing `HADOOP_HOME`/`winutils.exe`** — PySpark on Windows needs Hadoop's native Windows shims even for pure local mode.
2. **`PYSPARK_PYTHON` unset** → Spark's worker subprocess fell back to the Windows Store Python stub (does nothing, exits) → JVM's `accept()` call timed out. **Gotcha:** the fix path must use Windows backslashes, not Git Bash's `/d/...` form — Java's `ProcessBuilder` splits on whitespace when it doesn't recognize the path style, and the project directory itself has a space in it.
3. **`pytest` couldn't resolve a bare `import event_mapper`** — the target directory had an `__init__.py`, changing pytest's import-root resolution. Fixed via a `pythonpath` entry in `pyproject.toml`.

Result after fixing: 21/21 relevant tests passing.

### Bugs found — Terraform (code-review pass, before any `apply`)

- Storage account provisioned 8 containers but never `staging` — even though the ADF pipeline's own config pointed at `staging` as its landing zone. Would have failed at runtime with "filesystem not found."
- The smoke test only checked 6 of the (then 9) containers — could pass while part of the landing zone was silently missing.

### Bugs found — running `terraform apply` for real

- **App Configuration key writes hung 45 min, then failed** — writing App Config key-values needs the "App Configuration Data Owner" *data-plane* RBAC role, not covered by subscription Owner (a genuine Azure quirk, unlike Key Vault). If `az role assignment create` itself failed with `MissingSubscription` (a real CLI bug), used `az rest` directly instead.
- **Key Vault and Cosmos DB names collided globally** — both are DNS-style globally-unique across *all* Azure tenants; `kv-fraud-dev`/`cosmos-fraud-dev` were already taken by someone else. Fixed with a `random_string` suffix → real names `kv-fraud-dev-4th9`, `cosmos-fraud-dev-604t`.
- **RBAC module tried to grant roles to identities that don't exist yet** (Decision Function, Logic App — both Phase 5). Fixed with `count` guards, skipped when the principal ID is null.
- **Diagnostic-settings module hardcoded log/metric categories** that Storage accounts and Databricks workspaces don't actually support. Made toggleable.
- A permanent, cosmetic 4-resource `terraform plan` diff (storage `network_rules` + 3 diagnostics) — a known `azurerm` provider quirk, safe to ignore.

---

## Phase 1 — Batch Foundation: Bronze/Silver/Gold + Baseline Model

**Goal:** IEEE-CIS Kaggle dataset landed and transformed through the medallion architecture; XGBoost baseline trained and registered.

### Key decisions

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Timestamp anchor for `TransactionDT` | Fixed reference date | Reproducible across runs/environments |
| 3 | Numeric null handling | **Preserve NULL + `_is_null` flag** | Missingness IS a fraud signal; imputation destroys information |
| 5 | Train/test split | **Time-based (chronological)** | Random split leaks future fraud patterns → inflated metrics → production failure. Non-negotiable. |
| 7 | Primary metric | **PR-AUC** | ROC-AUC is misleading on imbalanced data (high AUC, poor precision) |
| 8 | Hyperparameter tuning | **Optuna (Bayesian)** | Grid search is combinatorially explosive; random doesn't converge |
| 11 | Silver idempotency | **MERGE (upsert)** on `transaction_id` | Safe to re-run without duplicates (vs. destructive `INSERT OVERWRITE`) |
| 13 | Target encoding leakage | **K-fold, fit on train only** | Global target encoding leaks label info across splits |
| 14 | Model calibration | **Platt scaling** | XGBoost raw probabilities aren't calibrated; Phase 5's decision thresholds need calibrated ones |

### Commands run

```bash
pip install databricks-cli
databricks configure --token   # token from workspace User Settings > Developer

# Unity Catalog
databricks sql -e "CREATE CATALOG IF NOT EXISTS fraud_detection_dev;
  CREATE SCHEMA IF NOT EXISTS fraud_detection_dev.bronze; ... silver; ... gold;"
# Storage credential + external locations (workspace's auto-provisioned
# "default" credential is locked to Databricks' own managed path — see bug below)
databricks sql -e "CREATE STORAGE CREDENTIAL cred_fraud_lake WITH (AZURE_MANAGED_IDENTITY = '<connector-id>');
  CREATE EXTERNAL LOCATION loc_bronze URL 'abfss://bronze@stfraudlakedev...' WITH (STORAGE CREDENTIAL cred_fraud_lake);"

databricks clusters start 0830-043110-gk2nx3tn   # 3-7 min to RUNNING

# Kaggle data
pip install kaggle
kaggle competitions download -c ieee-fraud-detection -p /tmp/ieee-cis
azcopy copy "/tmp/ieee-cis/train_transaction.csv" "https://stfraudlakedev.dfs.core.windows.net/bronze/ieee-cis/..."

# Run notebooks via the Databricks Command Execution REST API (create
# execution context, submit commands, poll status) instead of clicking
# "Run All" by hand, so runs could be monitored programmatically:
#   bronze/ingest_ieee_cis_transactions.py, ingest_ieee_cis_identity.py
#   silver/transform_ieee_cis_to_silver.py
#   gold/ (aggregation)
#   ml/training/train_xgboost_baseline.py
```

```sql
SELECT COUNT(*) FROM fraud_detection_dev.bronze.ieee_cis_transactions;  -- ~590,540
SELECT COUNT(DISTINCT event_date) FROM fraud_detection_dev.silver.transactions;  -- must be >1
```

### Bugs found — code review (before running)

- Bare import (`from feature_engineering import ...`) instead of the real package path — `ModuleNotFoundError` on any real run.
- **No `pyproject.toml` existed at all** — every `fraud_detection.*` import across the repo raised `ModuleNotFoundError`, and both CI workflows masked this with `|| echo "No tests found yet"` (tests silently never ran). Added the root `pyproject.toml`, fixed CI to stop swallowing failures.
- A hardcoded fraud-rate assertion (`0.02 <= fraud_rate <= 0.06`) would break the moment Phase 2's streaming data (≈0.17% fraud) landed in the same table — moved to a warning-level check with a broad sanity band.
- ADF's "manual" trigger was declared as `CustomEventsTrigger` with no required properties — invalid ARM schema. ADF has no true manual-trigger type; changed to a `ScheduleTrigger` deployed `Stopped`, started only on-demand via CLI.
- A "validation" pipeline that only checked file existence/size, never compared against expected schema — added real `If Condition`/`Fail` activities.

### Bugs found — running it for real

- `run_bronze_quality_gate()` was called but not importable → moved into `databricks/src/quality/quality_gate.py`.
- `pyproject.toml` required Python ≥3.11, but Databricks Runtime 14.3 ships 3.10.12 → relaxed to `>=3.10`.
- Auto Loader missing required `cloudFiles.schemaLocation`; given a literal file path instead of a directory + `pathGlobFilter`; `cloudFiles.badRecordsPath` isn't a real option key (it's `badRecordsPath`, no prefix).
- Silver notebook joined on `TransactionID` *after* the identity side had already been renamed to snake_case — join silently matched nothing.
- `_rescued_data` column collision after the bronze join (both sides had it).
- `mlflow.set_experiment("fraud-detection-baseline")` — bare name invalid on Databricks; needs an absolute workspace path.
- **The most serious bug in the whole project:** `unix_timestamp()` in `databricks/src/transformations/cleaning.py` couldn't parse the ISO8601 `REFERENCE_TIMESTAMP` string, silently returning `NULL` for every row's `event_date` — would have collapsed the entire Gold layer into 1 row instead of 182, and quietly broken every date-derived feature.
- `.toPandas()` on all ~470 silver columns OOM-killed the cluster → select only needed columns + downcast to `float32` before collecting.
- `mlflow.xgboost.log_model()` missing the signature Unity Catalog requires, and used a bare (non-three-level) registered model name.
- Missing `mlflow[databricks]` extras, needed specifically for UC model registry access.

---

## Phase 2 — Streaming: Event Hubs & Transaction Producer

**Goal:** live-replayed traffic flowing through Event Hubs into Bronze streaming ingestion.

### Key decisions

| # | Decision | Free Trial choice | Production upgrade | Rationale |
|---|---|---|---|---|
| 1 | Event Hubs tier | **Standard** ($11/mo) | Premium ($690/mo) | Premium adds Schema Registry, Kafka protocol, 90-day retention |
| 2 | Partition count | **4** | 32 | 4 matches single-node parallelism |
| 9 | Streaming cluster | **Single-node, 20-min auto-terminate** | Multi-node, no auto-terminate | Free Trial can't run 24/7 streaming |
| 15 | Partition key | **`card_id`** | Same | Per-card ordering is load-bearing for Phase 3 velocity features — non-negotiable |

### Commands run

```bash
KV_NAME=$(az keyvault list --resource-group rg-fraud-detection-dev --query "[0].name" -o tsv)
CONN_STR=$(az eventhubs eventhub authorization-rule keys list --resource-group rg-fraud-detection-dev \
  --namespace-name ehns-fraud-dev --eventhub-name eh-transactions --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)
az keyvault secret set --vault-name "$KV_NAME" --name eventhub-connection-string --value "$CONN_STR"

export EVENTHUB_CONNECTION_STRING="$(az keyvault secret show --vault-name <kv> --name eventhub-connection-string --query value -o tsv)"
.venv/Scripts/python.exe -m producers.transaction_producer.producer --speed-factor 1000 --total-events 20000
# ran in background; sent 20,000/20,000 events successfully
```

```python
# Late-arrival distribution measurement — directly informed Phase 3's watermark choice
df.withColumn("lateness_sec", F.col("enqueued_time").cast("long") - F.col("event_time_ts").cast("long")) \
  .select(F.expr("percentile_approx(lateness_sec, array(0.5, 0.9, 0.99))")).show()
```

### Bugs found — code review

- `event_data.properties = {"partition_key": ...}` writes an application-metadata property, **not** the actual Event Hubs partition key — every event was round-robined across partitions with no card affinity, directly contradicting decision #15 above. Fixed to use `create_batch(partition_key=card_id)`.
- `VALIDATE_SCHEMA=false` still evaluated `True` — `bool()` on any non-empty string is truthy in Python. Also, schema validation was never actually wired into the producer despite `jsonschema` being a declared dependency. Fixed both.

### Bugs found — running it for real

- `config.py`'s `time_anchor` was hardcoded to a fixed past date instead of defaulting to "now" — meaningless for measuring live late-arrival behavior, since every event would already be "old" by definition. Fixed to default to `datetime.now(timezone.utc)`.
- **A process-management lesson, not a code bug:** partway through the 20,000-event run, an empty output-tracking file was misread as "the process is stuck," and it was killed with `kill -9` — it was actually healthy, having already sent 10,000 events. Root cause: a manual stdout redirect meant the harness's own tracking file legitimately had nothing in it — the wrong file was being checked. **Lesson: when you add your own redirect, check that file, not the harness's default one.**

---

## Phase 3 — Feature Engineering, GraphFrames, Cosmos DB Graph

**Goal:** behavioral/merchant-risk/graph features computed off the real streaming table; a live transaction graph in Cosmos DB.

### Key decisions

| # | Decision | Free Trial choice | Production upgrade | Rationale |
|---|---|---|---|---|
| 1 | Feature store engine | **Delta Lake feature tables** | Azure ML Managed Feature Store | Avoids Azure ML standing compute cost |
| 2 | Online feature sink | **Delta lookup key-value index** | Azure Managed Redis (<10ms) | Avoids managed Redis hourly cost |
| 3 | Graph query engine | **Cosmos DB Gremlin (Serverless)** | Cosmos DB Gremlin Autoscale | Serverless = zero idle RU cost |
| 6 | Point-in-time join | **Custom as-of join (`_pit_rank`)** | Azure ML `get_offline_features` | Operates without the Azure ML SDK dependency |

### Commands run

```python
# GraphFrames connectedComponents() needs a checkpoint dir that bypasses
# Unity Catalog entirely (DBFS root is disabled on UC-enabled workspaces) —
# only works because the cluster is single-node:
spark.sparkContext.setCheckpointDir("file:/tmp/graphframes-checkpoints")
```

```python
# Verifying the graph directly via gremlin_python
g.V().count().next()   # vertex count growing
g.E().count().next()   # edge count growing
```

### The recurring "wrong table" bug (its first of 3 appearances)

`stream_silver_from_bronze.py` was merging live Event Hub data into the
**same** `silver.transactions` table the Phase 1 batch pipeline had already
populated from the static IEEE-CIS CSVs — two incompatible schemas. This
wasn't just wrong output, it **actively corrupted the Phase 1 baseline table
on every micro-batch**. Fixed by routing to a new `silver.streaming_transactions`
table, and updating every downstream feature notebook to read from it. This
exact class of bug (pointing at the wrong/legacy table) recurs in Phase 4's
`data_preparation.py`, Phase 6's `ingest_chargeback_feedback.py`, and Phase
7's masking policy SQL — five separate times across the project.

### Bugs found — code review

- `PAYMENT_METHOD_MAP`/`CHANNEL_MAP` used a made-up vocabulary that didn't match the actual JSON Schema's enums — any schema-valid `prepaid`, `bank_transfer`, `moto`, or `recurring` transaction silently fell into the "unknown" bucket, losing the signal entirely.
- **A genuine label-leakage architecture gap**, found while adding test coverage: `customer_behavioral_baselines`/`merchant_risk_baselines`/`graph_entity_metrics` were MERGE-upserted to hold only the *current* value, so a plain `.join()` against them attached today's value to a training row from 3 months ago — real leakage. Fixed by historizing all three (dated snapshots, PIT-joinable with a lookback window).
- Chaining multiple point-in-time joins with a repeated `entity_key` left duplicate column names → `AMBIGUOUS_REFERENCE`. No test existed to catch this; 5 new tests added.

### Bugs found — running it for real

- `compute_behavioral_baselines.py` referenced a nonexistent `event_date` column on the streaming table (it only has `event_time_ts`).
- `compute_merchant_risk.py` filtered `is_fraud == 1` against what's actually a boolean column — fixed to `== True`.
- `materialize_feature_store.py` was **entirely commented out** (matching the repo's own `TODO.md` admission) — rewritten for real.
- Cosmos DB edge-writer had 4 separate bugs: wrong secret scope name (`fraud-secrets` vs. the real `kv-fraud`); `gremlin_python`'s own asyncio event loop conflicting with the notebook kernel's (fixed with `nest_asyncio`); Cosmos DB Gremlin only speaks GraphSON 2.0 but the client defaults to 3.0 (connects, then the server silently closes it on the first real request); and fire-and-forget writes creating a real race condition (an edge could be submitted before its vertices existed) while silently swallowing errors.

---

## Phase 4 — Hybrid Ensemble & Serving

**Goal:** XGBoost + Autoencoder + Isolation Forest, stacked, packaged as one deployable model.

### Key decisions

| # | Decision | Free Trial choice | Rationale |
|---|---|---|---|
| 1 | Serving engine | **Local/Databricks MLflow PyFunc** | Avoids $3-5/day continuous cluster cost of an Azure ML endpoint |
| 3 | Unsupervised signal | **PyTorch Autoencoder + Isolation Forest** | Dual anomaly signals — deep reconstruction *and* tree partitioning |
| 4 | Ensemble combination | **Stacking meta-learner (logistic regression)** | Learns optimal combination weights dynamically |
| 5 | Score calibration | **Isotonic regression** | Normalizes heterogeneous scores; handles non-monotonic mappings |
| 7 | Explainability | **SHAP TreeExplainer** | Mandatory for PCI/regulatory decline-explanation audit |

### Commands run

```python
# Packaging the whole ensemble as ONE mlflow.pyfunc model, so a single
# load_model() call reconstructs everything at serving time — exactly how
# Azure ML (or any serving layer) would load it in production
mlflow.set_experiment("/fraud-detection-phase4-ensemble")
with mlflow.start_run(run_name="ensemble-pyfunc-packaging"):
    mlflow.pyfunc.log_model(
        name="ensemble_model",
        python_model=FraudEnsemblePyFunc(),
        artifacts={"xgb_model": ..., "ae_model": ..., "iso_forest": ...,
                   "cal_xgb": ..., "cal_ae": ..., "cal_if": ..., "meta_learner": ...},
    )

loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/ensemble_model")
result = loaded_model.predict(X_test[sample_idx:sample_idx + 1])
# -> {"fraud_probability": ..., "component_scores": {...}, "scoring_mode": "full"}
```

### Bugs found — code review

- `log_model()` was called without `registered_model_name` — the entire Phase 6 MLOps loop references a registered model that never actually got registered under any name.
- Deployment spec referenced a different model name than what was actually registered elsewhere.
- Hardcoded, wrong Azure ML model-mount path guess — replaced with `_resolve_model_path()`, which walks the mount dir looking for the actual `MLmodel` file.
- SMOTE augmentation crashed (`ValueError`) whenever the target ratio was at or below current prevalence, or there was zero real fraud to sample from — added an early skip instead of a crash.

### Bugs found — running it for real

- `data_preparation.py` referenced `gold.reconciled_labeled_transactions` — a table **no notebook in the entire pipeline ever produced**. Phase 4 training could not run at all until this was pointed at `silver.streaming_transactions` directly.
- A calendar-month train/val/test split — nonsensical for a compressed-timeframe streaming replay spanning minutes, not months. Replaced with a **70/15/15 chronological split**.
- The installed XGBoost 3.x needs `early_stopping_rounds` as a **constructor** parameter, not a `.fit()` kwarg — the code's own comment claimed the opposite (backwards for the actually-installed version).
- `mlflow.start_run(nested=True, ...)` needs an active parent run — fixed at the call site, not inside the reusable training function, to keep it composable.
- `ml/serving/score.py` had bare imports that only work as a standalone script.
- **An honestly-flagged red flag, not a bug fix:** a suspiciously perfect PR-AUC (1.0) was observed and explicitly called out as a likely artifact of the synthetic entity simulation (the dataset's most-predictive columns were deterministically hashed into `card_id`, effectively leaking the label into an ID field) — reported as such, not claimed as a genuine production-grade result.

### Verification performed

- Circuit breaker: full CLOSED→OPEN→HALF_OPEN→CLOSED state machine exercised with real assertions, including the "failed probe re-opens immediately" edge case.
- Fallback rules verified across 4 amount tiers ($100→0.05 approve, $6,000→0.85 manual review), always tagged `"scoring_mode": "rules_only_fallback"` so downstream systems never mistake an emergency rule score for a real model decision.

---

## Phase 5 — Decision Engine, Case Management, Compliance Audit

**Goal:** real-time scoring → routing (approve/step-up/block/review) with a full audit trail.

### Key decisions

| # | Decision | Free Trial choice | Rationale |
|---|---|---|---|
| 1 | Decision Engine | **Azure Functions (Consumption)** | Free for 1M requests/month |
| 3 | Case system of record | **Azure SQL Serverless** | Auto-pause eliminates idle billing |
| 4 | Duplicate handling | **SQL `MERGE` + Service Bus dup window** | Guarantees idempotency on at-least-once delivery |
| 6 | Threshold storage | **Azure App Configuration** | Dynamic threshold changes with **zero redeploy** |
| 7 | Audit retention | **Delta Table / ADLS append-only** | Regulatory compliance requires an immutable audit trail |

### Commands run

```bash
az sql server firewall-rule create --resource-group rg-fraud-detection-dev \
  --server sql-fraud-dev --name AllowMyDevIP --start-ip-address <ip> --end-ip-address <ip>
pip install pymssql   # bundles FreeTDS, no separate ODBC driver needed on Windows
# ran V001-V005 migrations + 2 stored procedures via a pymssql runner

# Deploying Azure Functions — config-zip does NOT build Python deps on
# current-gen Linux Consumption apps, even with SCM_DO_BUILD_DURING_DEPLOYMENT
pip install --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.11 \
  --target build_dir/.python_packages/lib/site-packages -r functions/<name>/requirements.txt
az functionapp deployment source config-zip --resource-group rg-fraud-detection-dev \
  --name <func-app-name> --src <built-zip-with-dependencies>

# Verify indexing via the admin API, NOT `az functionapp function list` (lags by minutes)
curl "https://<app>.azurewebsites.net/admin/functions?code=<masterKey>"

# Dynamic threshold change, zero redeploy — the whole point of App Config
az appconfig kv set --name appcs-fraud-dev --key "FraudEngine:StepUpMaxThreshold" --value "0.50"
```

### Bugs found — code review

- `sp_upsert_fraud_case` inserted an `event_type` value that violated its own CHECK constraint — **every single call failed**, so no fraud case could ever be created.
- `MERGE` without `WITH (HOLDLOCK)` under default isolation allowed two concurrent calls for a new `transaction_id` to both pass `WHEN NOT MATCHED`, racing on the unique constraint (a known SQL Server `MERGE` race).
- Audit JSON built via raw string concatenation with no escaping — free-text analyst notes containing `"` or `\` produced malformed JSON or let an analyst inject arbitrary fields into an immutable audit trail. Fixed with `STRING_ESCAPE`.
- DLQ code passed `f"{sub_name}/$DeadLetterQueue"` as the subscription name — the SDK doesn't support that; DLQ access needs the separate `sub_queue=ServiceBusSubQueue.DEAD_LETTER` kwarg. **DLQ monitoring and replay were both silently non-functional** despite looking wired up (caught by a broad `except`).
- The Logic App's `Parse_Message_JSON` fed base64-encoded Service Bus content straight into `ParseJson` without decoding it first — every message would have failed validation immediately.
- `sub-case-mgmt` (catches `manual_review` decisions) had **no consumer anywhere in the repo** — a `manual_review` decision never got a case created in Azure SQL. Added a new minimal Logic App workflow to close this.
- **Honestly-documented, deliberately-unbuilt scope:** the plan's own diagram describes a full OTP/analyst-assignment workflow (SMS delivery, branching on response, round-robin assignment, auto-escalation). None of it exists — no Twilio integration, no callback API, no analyst roster. Rather than fabricate an untestable subsystem, the existing "wait 5 min, then unconditionally escalate" fallback was kept and honestly labelled as the conservative timeout path the diagram itself specifies — not silently presented as the full flow.

### The one genuine production bug found only by actually deploying

**`audit_logger` wrote 0-byte compliance records**, and every delivery
dead-lettered after 10 retries. Root cause: `_persist_audit_record()` called
`create_file()` (creates an empty file) then `upload_data(overwrite=False)`
on that same now-already-existing file — refused as "already exists," 0
bytes written. Fixed with an explicit existence check before writing, instead
of relying on `overwrite=` to double as both "create if absent" and a dedup
signal. Verified end-to-end afterward: a real 514-byte JSON audit record,
zero dead-letters; then replayed the 6 stale dead-lettered messages from
testing against the broken code — all consumed cleanly.

### Verification performed (§5.12 checklist)

- Decision Function latency: 0.14-0.2ms warm; ~85ms on the first call after a threshold-cache refresh (App Config round-trip).
- Score 0.05 → 200/approve, no publish. Score 0.95 → 403/block, event published and consumed downstream.
- Threshold changed via `az appconfig kv set` → a boundary score correctly reclassified on the very next request, zero redeploy.

---

## Phase 6 — MLOps Loop: Drift, Retraining, Champion/Challenger, Rollback

**Goal:** the full closed loop — label reconciliation → drift detection → retraining → gated promotion → shadow scoring → rollback.

### Key decisions

| # | Decision | Free Trial choice | Rationale |
|---|---|---|---|
| 1 | Drift metrics | **PSI + KS-test + Jensen-Shannon divergence** | Multi-metric catches drift a single metric would miss |
| 3 | Retrain trigger | **PSI ≥ 0.25 or bi-weekly schedule** | Daily scheduled job (vs. an Event Grid webhook) |
| 4 | Shadow scoring | **Async Delta batch log** | Avoids running dual live scoring endpoints |
| 5 | Evaluation gate | **4-gate + McNemar's significance test** | Industry-standard multi-criteria promotion, not just "is it better" |
| 8 | Rollback sentinel | **Live telemetry query from the Gold KPI table** | Queries real metrics, not hardcoded dummy values |

### Commands run

```bash
export DATABRICKS_HOST=...   # a fresh environment had lost its saved CLI auth;
                              # fell back to Azure CLI federation (az login's
                              # session), no new PAT needed

# The Command Execution API's Python context has none of the ML stack
# pre-installed — rebuild and force-reinstall the project's own wheel
python -m build --wheel
# upload as a workspace file (DBFS root disabled) then, in-notebook:
# %pip install --force-reinstall --no-deps <path>, purging sys.modules first
```

```sql
-- The 'gold' container had no external location at all (Phase 3 only ever
-- registered raw/checkpoints/quarantine; gold tables were always written
-- via saveAsTable, never read by raw abfss:// path until Phase 6 needed to)
CREATE EXTERNAL LOCATION loc_gold URL 'abfss://gold@stfraudlakedev...'
  WITH (STORAGE CREDENTIAL stfraudlakedev_credential);
```

### The most consequential bug of this phase: a feature-schema mismatch

Three files independently computed `feature_cols` by prefix-matching column
names, exactly as the plan specified — but the **actually-registered**
Champion's real training schema was 30 specific columns, not a naive prefix
match (which also would have swept in two raw timestamp columns the model
can't cast to float). The prefix-only version silently produced a 3-column
feature set. This didn't fail quietly: `champion_challenger_gate.py` crashed
outright — `Shape of input (1, 3) does not match expected shape (-1, 30)`.
Fixed by defining the feature set identically across all three files, and
adding the point-in-time join call two of them had never actually included.

### Other real bugs found by running the code

| File | Bug | Fix |
|---|---|---|
| `retrain_pipeline.py` | `/dbfs/tmp/...` → `OSError` (DBFS FUSE mount disabled) | Switched to local driver disk |
| `retrain_pipeline.py` | sklearn/torch don't accept NaN natively (unlike XGBoost); no NaN guard at training time even though inference already had one | Added `np.nan_to_num(...)` matching the inference-time guard |
| `retrain_pipeline.py` | No model `signature=` → Unity Catalog rejected the version, but only much later, deep inside `champion_challenger_gate.py` | Built a real signature via `infer_signature` at log time |
| multiple | NaN guard cast to `float64`, but evaluation code built arrays as `float32` → dtype mismatch on every real scoring call | Standardized on `float32` everywhere |
| `shadow_scoring_batch.py` | `pandas_udf` given 30 columns but its function signature only handled 1 | Replaced the whole vectorized approach with the same reliable per-row-loop pattern already used elsewhere (no vectorization was being gained anyway) |
| `shadow_scoring_batch.py` | `amount` selected twice → `AMBIGUOUS_REFERENCE` on `MERGE INTO` | Removed the redundant explicit column |
| 2 files | `scipy.stats.binom_test` removed in the installed scipy version | Switched to `binomtest(...).pvalue` |

### A performance ceiling, accepted rather than engineered around

`FraudEnsemblePyFunc.predict()` has no true batch path — every evaluation
scores one row at a time in a Python loop. A first attempt at scoring tens
of thousands of rows ran 7+ minutes with no end in sight; cancelled and
**capped both gate and shadow-scoring evaluations to a random 2,000-row
sample** — still statistically meaningful for PR-AUC/recall/FPR estimation, finishes in under a minute.

### Verification performed (§6.13 checklist) — a real end-to-end loop

- Label reconciliation ran 3x idempotently (31,265 rows, no duplicates); priority ordering proven across two real incremental `MERGE`s (`ANALYST_CONFIRMED_LEGIT` correctly overridden by a later `CHARGEBACK`).
- Full ensemble retrained twice for real (Optuna, 30 trials); challenger PR-AUC 0.6364, McNemar's p=0.0000 — **strictly dominated** the champion on the eval sample; all 4 gates passed → version 2 promoted to `@champion` for real.
- Rollback sentinel: healthy-state run correctly reported no anomaly; a seeded synthetic anomaly (FPR 0.31 vs. baseline 0.01) correctly triggered a real rollback, reverting `@champion` from v2 back to v1.
- **A genuine, honestly-reported limitation, not a bug:** this dev environment has exactly one real reconciled label. To exercise the retraining path at all, a sample of pending rows was synthetically "matured" (reusing each row's own streaming-replay `is_fraud` flag), clearly tagged `label_source='SYNTHETIC_TEST_MATURED'` — a real characteristic of a sandbox with no real payment processor, not something more code could fix.

---

## Phase 7 — Governance, Security & Hardening

**Goal:** PII protection, RBAC, automated security scanning, chaos resilience, and a real credential rotation.

### Key decisions

| # | Decision | Free Trial choice | Rationale |
|---|---|---|---|
| 1 | Governance catalog | **Unity Catalog masking + row filters** | Native masking without Purview's cost |
| 2 | PII protection | **SHA-256 hashing, Key Vault-salted** | Hashes at the Bronze→Silver boundary, with automated validation |
| 3 | Network security | **Service firewalls + IP rules** | Private endpoints cost ~$7.20/mo per service |
| 6 | Security automation | **4-scan pipeline**: TruffleHog + Checkov + Bandit + pip-audit | Secret, IaC, SAST, and CVE scanning in one CI gate |
| 8 | Data retention | Bronze indefinite, Silver 3yr, Gold/Audit 7yr | PCI-DSS 10.7 retention requirements |

### Commands run

```bash
az functionapp identity show --name func-fraud-decision-dev \
  --resource-group rg-fraud-detection-dev --query principalId -o tsv
# added the real GUID to dev.tfvars, applied — 2 real RBAC role assignments created
```

```bash
# 4-scan security pipeline, run locally against the real codebase
trufflehog3 filesystem .                 # secrets — 0 real leaks
checkov -d infrastructure/                # IaC
bandit -r databricks/src ml/              # Python SAST
pip_audit                                 # dependency CVEs
```

```python
# PII hashing verification — both directions
sanitize_pii_fields(df)          # real 64-char SHA-256 hashes
validate_pii_hashing(hashed_df)  # -> True
validate_pii_hashing(raw_df)     # -> ValueError on all 1,000 rows (negative test)
```

### The most severe bug found in this phase's code review

`rotate_keyvault_secrets.py` generated a new random password and wrote it to
Key Vault — but **never actually changed the corresponding credential on
the live system**. Rotation would have made Key Vault hold a value that no
longer matched the real Azure SQL Server password, breaking every service
authenticating with that secret immediately after a "successful" rotation.
Fixed to update the live server password *first* via the ARM API, and only
write Key Vault once that succeeds — so a failed rotation can never leave
Key Vault out of sync with the live server.

### A near-miss caught before it caused damage

The first `terraform plan` after wiring real RBAC showed all 3 Function
Apps "changing" — Terraform wanted to **delete**
`WEBSITE_RUN_FROM_PACKAGE` (the pointer to the currently-deployed code) from
all 3, because that setting was applied out-of-band by the deployment
process and the Terraform module never declared it. Caught by reading the
actual diff before applying (not just running `apply`), fixed with
`lifecycle { ignore_changes = [app_settings] }`.

### Unity Catalog masking: worked, then broke something else, then got fixed properly

Column masks require a Shared-mode SQL Warehouse — the project's own
`SINGLE_USER` cluster explicitly rejects row/column policies. Applied via
the SQL Warehouse instead — **verified working**: queried the table as a
non-privileged user, got back `ip_address='.xxx.xxx'`. Then found a serious
compatibility break: applying the mask attached a `COLLATE` type annotation
that the older interactive cluster's SQL parser couldn't read back — even
for a plain `spark.table()` call, breaking every downstream pipeline script
that depends on that table. Masks were dropped to restore pipeline
functionality, **proven-but-inactive** — this was the honest, correct call
given the constraint, not a shortcut.

### Chaos resilience suite — run against the real live Decision Engine

| Test | Result |
|---|---|
| Corrupted payload → graceful fallback | ✅ PASS |
| Missing required fields → graceful fallback | ✅ PASS |
| Sequential p99 < 100ms | ⚠️ Found a real methodology bug, still failed after fixing the methodology — for a documented, non-blocking reason (see below) |
| 50-concurrent storm, ≥95% success | ✅ PASS (50/50) |
| All 10 score-boundary routings correct | ✅ PASS |

The latency test originally measured client-side wall-clock round-trip time
— dominated by network RTT, not anything the Function's own code controls.
Fixed to assert on the function's own self-reported `latency_ms` field
instead; still exceeded the 100ms bound at p99=178.97ms, explained by the
(already-documented) 60-second App Config threshold-cache legitimately
refreshing partway through a 100-request run. **This is expected behavior
of a deliberate caching design** — correctly not treated as a regression.
(This got properly root-caused and fixed for real in the follow-up session
below, rather than left as an accepted quirk forever.)

### Verifying against the Phase 7 checklist

13/17 fully verified, 4/17 verified with an honestly-documented caveat
(PR-AUC provenance, UC masking reverted for compatibility, 1 non-blocking
chaos test, an Entra→UC account-level group-sync gap unreachable
headlessly) — **zero items were silently marked done without real verification.**

---

## Follow-up Session — Closing 3 Previously-Documented Gaps

Three things were deliberately left open after Phase 7 and tracked in
`docs/operational_readiness_signoff.md`. This session closed all three —
worth reading on its own as an example of going back and finishing
documented technical debt rather than letting it calcify.

### 1. Logic Apps: deployed for the first time

No Terraform module had ever provisioned them — the workflow JSON existed
as plain files only. Added a real module (reads the JSON via
`jsondecode(file(...))` rather than duplicating it into HCL, so the
deployed workflow can never drift from what's committed). Two real bugs
found by actually deploying:
- Terraform's `depends_on` can't be a dynamic expression — a `for_each`-created action chain applied in parallel by default, but the Logic Apps API rejects an action whose `runAfter` target doesn't exist yet. Fixed by switching to one explicitly-named resource per action with a static `depends_on` chain.
- The workflow JSON used the *short* SQL server name in the connector path instead of the FQDN — the SQL connector's dataset path is the actual connection target, not a label.

Also hit a real state/reality drift: Phase 7's password rotation had updated
the live SQL server directly via the ARM API, bypassing Terraform — so
Terraform's state still held the pre-rotation password and wanted to reset
it. Resolved by re-reading the current password from Key Vault before
applying. **This drift will recur on every future `apply`** for as long as
rotation happens outside Terraform.

### 2. Chaos `test_03`: fixed the actual root cause, not the test

Rather than leave the 60-second-cache-refresh latency blip as a permanently
accepted quirk, the real fix: `_get_thresholds()` was doing a **synchronous,
blocking** App Config call inline whenever the cache expired — a genuine
design flaw (a hot request path has no business blocking on a periodic
config refresh). Fixed with a stale-while-revalidate pattern: kick off the
refresh on a background thread, return the cached value immediately; only a
cold start with no cached value yet still blocks. Result after redeploying:
p99 latency dropped from 178.97ms to **1.38ms**. All 5/5 chaos scenarios
now pass (previously 4/5).

### 3. Unity Catalog masking: re-enabled via a view, not a table-level mask

Instead of accepting "masking works but breaks the older cluster" as final,
created `silver.streaming_transactions_masked` — a **view** that computes
the same masking functions at query time, leaving the base table's stored
schema completely untouched. Verified both halves this time: the view
correctly masks PII when queried via the SQL Warehouse, *and* the raw table
is provably unaffected when read from the old interactive cluster
(`spark.table(...).count()` → `31265`, no error) — the exact reproduction
of the original break, now passing.

---

## Patterns worth internalizing (recurring across the whole project)

These aren't one-off bugs — they're the same class of mistake recurring,
which is itself the most useful thing to learn from this build:

1. **"Wrong table" bugs (5 separate occurrences)** — new code pointing at an
   old/legacy table instead of the current one (Phase 3's streaming merge,
   Phase 4's training data, Phase 6's chargeback feedback, Phase 7's masking
   policy). Every medallion-architecture project needs a single, obvious
   source of truth per concept, or this recurs forever.
2. **Bare imports vs. package imports** — code that only works when run as a
   standalone script from its own directory (`from foo import bar` instead
   of `from fraud_detection.x.foo import bar`) shows up in nearly every
   phase. Root cause: no `pyproject.toml` existed until Phase 1's fix.
3. **Hardcoded resource names that should have been resolved dynamically** —
   Key Vault name, Function App name, SQL server name all got hardcoded
   without their real random suffix/actual name at least 4 separate times
   across scripts written at different phases.
4. **"Written but never run" as the default state of most code** — nearly
   every phase's starting state was "this code exists in the repo, passed a
   code review, and has never once executed against real infrastructure."
   The overwhelming majority of real bugs were only found by actually
   running the code, not by reading it — code review caught the *obvious*
   bugs; execution caught the *real* ones.
5. **Honest reporting over silent success** — a recurring discipline
   throughout: a suspiciously perfect metric (Phase 4's PR-AUC=1.0), an
   unbuilt subsystem (Phase 5's OTP workflow), a reverted fix (Phase 7's
   masking), and partial verification (DLQ monitor's log content) were all
   explicitly flagged as such rather than glossed over.

---

## What's genuinely not done (as of the follow-up session)

- Azure ML Managed Online Endpoint deployment — deliberately out of scope
  for this Free Trial architecture (see `docs/phase0/upgrade_to_production.md`).
- Application Insights was never provisioned — Functions run without
  centralized log aggregation.
- The Entra ID → Unity Catalog account-level group sync — `GRANT` statements
  remain untested against a real group; UC resolves grants against
  Databricks *account*-level identity, which needs Account Console-level
  SCIM configuration unreachable from a headless CLI session.
- The full OTP/analyst-assignment step-up workflow described in the Phase 5
  design diagram — genuinely never built anywhere in the repo, by design
  (see Phase 5 above).
