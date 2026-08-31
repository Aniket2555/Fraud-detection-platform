# Execution Log — First Real Deployment & Validation Pass

This is a record of the first time this repository's code was actually run
against real Azure infrastructure, real data, and a real Databricks cluster
— as opposed to just being written. Before this session, per the repo's own
`TODO.md`: zero Azure resources existed, zero models were trained, and the
local test suite didn't even run.

Read this file first — it's the index and the quick-reference for resource
names, links, and the full bug list. Each phase has its own file with exact
commands and step-by-step manual instructions.

## Contents

| File | Covers |
|---|---|
| [01-local-environment.md](01-local-environment.md) | Fixing the local dev environment (PySpark, packages, pytest) |
| [02-infrastructure.md](02-infrastructure.md) | Deploying Azure infra via Terraform, GitHub Actions CI secrets |
| [03-data-landing.md](03-data-landing.md) | Databricks workspace setup, Kaggle data, Bronze→Silver→Gold, baseline model |
| [04-streaming.md](04-streaming.md) | Event Hubs, the transaction producer, streaming ingestion, late-arrival calibration |
| [05-feature-engineering.md](05-feature-engineering.md) | Feature store materialization, GraphFrames, Cosmos DB graph |
| [06-model-ensemble.md](06-model-ensemble.md) | Full hybrid ensemble training, MLflow packaging, scoring path, circuit breaker |
| [07-decision-engine.md](07-decision-engine.md) | Phase 5: Service Bus/SQL/App Config deployment, Azure Functions, a real audit-logger bug found and fixed, DLQ replay |
| [08-mlops-loop.md](08-mlops-loop.md) | Phase 6: drift monitoring, retraining, Champion/Challenger gates + promotion, shadow scoring, a real rollback |
| [09-governance-security.md](09-governance-security.md) | Phase 7: Unity Catalog masking, PII hashing, RBAC, 4-scan security pipeline, chaos tests, platform verification, a real Key Vault/SQL password rotation |

## Environment quick-reference

| Thing | Value |
|---|---|
| Azure subscription | `800df714-bea7-4580-8606-22b36ebee0fa` ("Azure subscription 1") |
| Azure tenant | `ce93da9d-d7b7-4db6-a625-8642417af178` |
| Resource group | `rg-fraud-detection-dev` |
| Terraform state resource group | `rg-tfstate-fraud-dev` |
| Terraform state storage account | `sttfstatedevmvm94i` |
| Storage account (data lake) | `stfraudlakedev` |
| Key Vault | `kv-fraud-dev-4th9` (name has a random suffix — see `02-infrastructure.md`) |
| Databricks workspace | https://adb-7405619338601349.9.azuredatabricks.net |
| Databricks cluster | `0830-043110-gk2nx3tn` (`batch-etl-dev`, `Standard_D4s_v5`, single-node) |
| Unity Catalog catalog | `fraud_detection_dev` |
| Cosmos DB account | `cosmos-fraud-dev-604t` |
| Event Hubs namespace / hub | `ehns-fraud-dev` / `eh-transactions` |
| Azure SQL server | `sql-fraud-dev.database.windows.net` |
| App Configuration | `appcs-fraud-dev` |
| CI service principal | `sp-fraud-detection-dev-ci` (app ID `1226f24c-778b-4478-b186-2b56895b6c30`) |
| GitHub repo | `Aniket2555/Fraud-detection-platform` |

## MLflow experiments & registered models

| Name | Link |
|---|---|
| `/fraud-detection-baseline` | https://adb-7405619338601349.9.azuredatabricks.net/ml/experiments/3667937819264216 |
| `/fraud-detection-phase4-ensemble` | https://adb-7405619338601349.9.azuredatabricks.net/ml/experiments/1166970400119118 |
| Registered model `fraud_detection_dev.gold.fraud_xgboost_baseline` | https://adb-7405619338601349.9.azuredatabricks.net/explore/data/models/fraud_detection_dev/gold/fraud_xgboost_baseline |

## Cluster is not always running

The Databricks cluster auto-terminates after a period of inactivity (last
set to 60 minutes). If a link above 404s or a `databricks clusters get`
call shows `TERMINATED`, start it again — see "Starting/using the cluster
manually" in `03-data-landing.md`. Starting takes 3-7 minutes.

## Full bug list (40 real defects found and fixed)

Every one of these was found by actually running the code — not by
inspection. All are fixed in the working tree as of this log; none are
committed to git yet (see "What's not done" below).

### Infrastructure / Terraform
1. `scripts/store_eventhub_secrets.sh` — hardcoded Key Vault name; the real vault has a random suffix
2. `databricks/workspace-setup/secret_scope_setup.sh` — same hardcoded-name bug (fixed pre-emptively)
3. `infrastructure/modules/key-vault/main.tf` — Key Vault names are globally unique across all Azure tenants; `kv-fraud-dev` collided with someone else's vault → added a random suffix
4. `infrastructure/modules/cosmos-db/main.tf` — same global-uniqueness collision → added a random suffix
5. `infrastructure/modules/rbac-assignments/main.tf` — 6 role assignments were unconditional even for identities (Decision Function, Logic App, Databricks storage) that don't exist yet → guarded with `count`
6. `infrastructure/modules/diagnostic-settings/main.tf` + `infrastructure/main.tf` — hardcoded log/metric categories that Storage accounts and Databricks workspaces don't support → made toggleable

### Phase 1 — Bronze/Silver/Gold + baseline model
7. `databricks/notebooks/silver/transform_ieee_cis_to_silver.py` — called `run_bronze_quality_gate()`, which existed but was never importable → moved into `databricks/src/quality/quality_gate.py`
8. `pyproject.toml` — `requires-python = ">=3.11"` but Databricks Runtime 14.3 ships Python 3.10.12 → relaxed to `>=3.10`
9. `databricks/notebooks/bronze/ingest_ieee_cis_transactions.py` + `ingest_ieee_cis_identity.py` — Auto Loader missing required `cloudFiles.schemaLocation`
10. Databricks' auto-provisioned "workspace default" storage credential (`dbw_fraud_dev`) is hard-restricted to its own managed path — registered a new storage credential + 3 external locations for our own storage account
11. Auto Loader `.load()` was given a literal file path instead of a directory + `pathGlobFilter`
12. `cloudFiles.badRecordsPath` isn't a real Auto Loader option key — it's `badRecordsPath` (no `cloudFiles.` prefix)
13. Silver notebook joined on `TransactionID` after the identity side had already been renamed to snake_case — the join silently matched nothing
14. `_rescued_data` column collision after the bronze join (both sides had it)
15. `mlflow.set_experiment("fraud-detection-baseline")` — bare name invalid on Databricks; needs an absolute workspace path
16. **The most serious one:** `unix_timestamp()` in `databricks/src/transformations/cleaning.py` couldn't parse the ISO8601 `REFERENCE_TIMESTAMP` string, returning `NULL` — silently collapsed every transaction's `event_date` into one bucket. Would have produced a Gold layer with 1 row instead of 182, and any date-derived feature would have been quietly wrong.
17. `ml/training/train_xgboost_baseline.py` — `from utils.feature_engineering import ...` (bare import, only works as a standalone script) → fixed to the proper package path
18. `.toPandas()` on all ~470 silver columns caused an OOM kill on the cluster → select only needed columns + downcast to float32 before collecting
19. `mlflow.xgboost.log_model()` was missing the signature Unity Catalog requires, and used a bare (non-three-level) registered model name
20. Missing `mlflow[databricks]` package extras, needed specifically for Unity Catalog model registry access

### Phase 2 — Streaming
21. `producers/transaction_producer/config.py` — `time_anchor` was hardcoded to a fixed past date instead of defaulting to "now", making any live-replay late-arrival measurement meaningless

### Phase 3 — Feature engineering, GraphFrames, Cosmos DB
22. **Critical data-corruption bug:** `databricks/notebooks/silver/stream_silver_from_bronze.py` was merging the live Event Hub dataset into the *same* `silver.transactions` table as the Phase 1 IEEE-CIS baseline data — two completely incompatible schemas. Fixed by routing to a new `silver.streaming_transactions` table.
23. `compute_behavioral_baselines.py` — same wrong-table bug, plus `event_date` doesn't exist on the streaming table (used `event_time_ts` instead)
24. `compute_merchant_risk.py` — same wrong-table bug, plus `is_fraud == 1` doesn't match a boolean column (fixed to `== True`)
25. `compute_graph_metrics.py` — same wrong-table bug
26. GraphFrames' `connectedComponents()` needs `sparkContext.setCheckpointDir()`, which bypasses Unity Catalog entirely and doesn't work with UC Volumes or DBFS root (disabled on this workspace) → used local disk (valid since the cluster is single-node)
27. `stream_edges_to_cosmos.py` — wrong secret scope name (`fraud-secrets` instead of `kv-fraud`)
28. `gremlin_python`'s client runs its own event loop, which conflicts with the notebook kernel's already-running one → needed `nest_asyncio`
29. Cosmos DB's Gremlin API only supports GraphSON 2.0; the client defaults to 3.0 — connects, then gets silently closed by the server on the first real request
30. `upsert_edge()`'s three Gremlin submissions were fire-and-forget (no `.all().result()`) — a real race condition (edge creation could run before its vertices existed) and silently swallowed errors

### Phase 4 — Hybrid ensemble & serving
31. `databricks/notebooks/features/materialize_feature_store.py` — entire body was commented out (already flagged in `TODO.md`) and referenced the wrong table — implemented for real
32. `ml/training/data_preparation.py` — referenced `gold.reconciled_labeled_transactions`, a table never produced anywhere in the pipeline → uses `silver.streaming_transactions` directly
33. `ml/training/data_preparation.py` — split data by calendar month (assumed a multi-month dataset) — nonsensical for a compressed-timeframe streaming replay → replaced with a 70/15/15 chronological split
34. `ml/training/train_supervised.py` — `early_stopping_rounds` passed to `.fit()`, but the installed XGBoost (3.x) requires it as a constructor parameter (the code's own comment claimed the opposite) — fixed in both the Optuna objective and the final model fit; also removed the now-invalid `use_label_encoder` param
35. `train_xgboost_supervised()`'s Optuna trials call `mlflow.start_run(nested=True, ...)`, which needs an active parent run and a set experiment — neither existed by default; fixed at the call site (driver script), not inside the reusable function
36. `ml/serving/score.py` — bare imports (`from shap_explainability import ...`, `from circuit_breaker import ...`) → fixed to package paths

## What's not done / not committed

- **None of these fixes are committed to git yet.** Everything above is a
  working-tree change. Run `git status` / `git diff` to review before
  committing.
- Azure ML Managed Online Endpoint deployment (Phase 4.6) — deliberately
  out of scope; this Free Trial architecture excludes Azure ML entirely
  (see `docs/phase0/upgrade_to_production.md`). Everything up to that
  deployment step (the model, its packaging, the exact scoring/fallback/
  circuit-breaker logic) is built and verified.
- Phase 5 onward (Decision Engine, MLOps loop, governance/hardening) —
  not started this session.
- The `1226f24c-...` CI service principal's client secret was shown once
  during creation and is in the GitHub Actions secrets — it is not
  recorded anywhere else. If lost, reset it with
  `az ad sp credential reset --id 1226f24c-778b-4478-b186-2b56895b6c30`
  and update the `AZURE_CLIENT_SECRET` GitHub secret.
