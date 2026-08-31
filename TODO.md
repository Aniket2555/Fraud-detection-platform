# Project TODO — Production Readiness Gap List

Generated 2026-08-27 from a full repo audit. Docs/README claim "100% complete," but that
reflects planning + code authoring only. **Nothing has been deployed, trained, or verified
against real Azure/data yet.** Overall real-world completion: **~45%**.

| Layer | Status | % |
|---|---|---|
| Planning docs / architecture | Fully written, 8 phases | 100% |
| IaC code (Terraform modules) | 13 of ~19 planned modules built | ~70% |
| App/ML/pipeline code | Written, no stubs found | ~90% |
| Azure deployment | Zero resources exist (`terraform apply` never run) | 0% |
| Secrets/credentials | All placeholders, nothing provisioned | 0% |
| ML model artifacts | No trained models exist (no `.pkl`/`.joblib`/mlruns) | 0% |
| Local dev environment | Only 3 of ~30 required packages installed | ~10% |
| Tests actually run | 12 failed / 5 passed (PySpark env broken) | ~15% |
| CI/CD proven working | Never run end-to-end (secrets missing) | 0% |
| Operational readiness sign-off | 0 of 17 checklist items checked | 0% |
| UI/Dashboard | Doesn't exist — likely intentional (backend-only design) | N/A |

---

## Blocking — nothing works until these are done

- [ ] Fill real values into `infrastructure/environments/dev.tfvars`
      (currently `owner_email = "your-email@example.com"`, `deployer_object_id = "YOUR-OBJECT-ID"` placeholders)
- [ ] Provision a real `sql_admin_password` and export as `TF_VAR_sql_admin_password`
- [ ] Set the ~11 required GitHub Actions secrets referenced in `.github/workflows/infra-deploy.yml`:
      `AZURE_SUBSCRIPTION_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
      `TFSTATE_RESOURCE_GROUP`, `TFSTATE_STORAGE_ACCOUNT`, `OWNER_EMAIL`, `DEPLOYER_OBJECT_ID`,
      `SQL_ADMIN_PASSWORD`, `DECISION_FUNCTION_PRINCIPAL_ID`, `LOGIC_APP_PRINCIPAL_ID`
- [ ] Run `terraform init/plan/apply` for real (bootstrap backend first) — zero Azure resources exist today
- [ ] After the Event Hubs module applies, run `scripts/store_eventhub_secrets.sh` (script is ready, never triggered)
- [ ] Fix local dev venv — install the ~27 missing packages pinned in `requirements.txt` / `ml/requirements.txt`:
      `xgboost`, `torch`, `mlflow`, all `azure-*` SDKs, `jsonschema`, `requests`, `shap`, `optuna`,
      `pytest-mock`, etc. (only `pandas`, `pyspark`, `scikit-learn` currently installed)

## Validation — prove the code actually works

- [ ] Fix local PySpark on Windows — every Spark-based feature test currently fails with
      `Py4JJavaError: SocketTimeoutException: Accept timed out` (JVM↔worker socket issue,
      likely missing `HADOOP_HOME`/`winutils.exe` or a firewall blocking loopback).
      Consider running feature tests inside WSL/Docker instead.
      Affected: `test_cleaning.py`, `test_geo_features.py`, `test_point_in_time_join.py`,
      `test_stateless_features.py`, `test_velocity_features.py` (12 failing / 5 passing today)
- [ ] Run the ML pipeline end-to-end at least once — produce real trained model artifacts
      (currently none exist anywhere: no `.pkl`, `.joblib`, `.onnx`, no `mlruns/`)
- [ ] Get full `pytest` suite collecting/passing — currently fails on missing imports
      (`jsonschema`, `requests`, etc.)
- [ ] `databricks/notebooks/validation/test_feature_consistency.py` is a notebook script, not a
      real unit test — pytest wrongly tries to collect it and it assumes a live Databricks
      session. Exclude from test collection or convert it properly.
- [ ] Run `tests/chaos/test_resilience_scenarios.py` against real deployed infra
- [ ] Run each CI/CD workflow (`infra-deploy`, `data-ci`, `ml-ci`, `security-scan`) end-to-end
      at least once and confirm green

## Code vs. architecture divergences (confirmed by code audit 2026-08-29)

A stage-by-stage check of the codebase against `Implementation-details/fraud_detection_pipeline_architecture.md`
found the implementation is largely faithful (real PyDeequ gates, real GraphFrames + Cosmos DB Gremlin graph
features, all four ML model components, App-Config-driven decision thresholds, idempotent SQL MERGE, real PSI
drift detector) — but two genuine gaps exist:

- [ ] **No online feature store (Redis) client code anywhere in the repo.** `ml/serving/score.py` accepts
      `online_features` as a plain dict handed to it in the request payload — nothing anywhere actually
      fetches it from Azure Managed Redis (grep for `redis`/`FeatureStoreClient` across `ml/`, `databricks/`,
      `functions/` returns zero hits). The offline/graph halves of the feature store are real; the online
      half is a documented contract with no producer-side implementation. Also,
      `databricks/notebooks/features/materialize_feature_store.py` has its actual materialization calls
      commented out.
- [ ] **Logic App step-up workflow doesn't branch on real OTP/biometric response.**
      `logic-apps/workflows/workflow_stepup_auth.json` (line 82) self-documents that "no customer response
      mechanism is implemented yet" — every step-up case is conservatively escalated straight to manual
      review after the wait, rather than branching on an actual customer response as designed.

## Missing infrastructure modules (planned in Phases.md, not yet built)

- [ ] `azureml-workspace`
- [ ] `vnet`
- [ ] `managed-redis`
- [ ] `logic-app`
- [ ] `purview`
- [ ] `alerts`
- [ ] `staging.tfvars` / `prod.tfvars` (only `dev.tfvars` exists)

## Sign-off / documentation honesty

- [ ] `docs/operational_readiness_signoff.md` — all 17 rows unchecked `☐`, sign-off table has
      no names/dates. Work through it for real, don't leave it as a template.
- [ ] `README.md` "100% Complete 🎉" phase status is misleading as written — reflects
      docs/code authored, not deployed/validated. Caveat it or split into
      "planning: done" vs "deployed/validated: not started".

## Needs a decision (not clearly a gap — may be intentional scope)

- [ ] **UI/Dashboard** — nothing exists anywhere in the repo (no `package.json`, no
      `.tsx`/`.jsx`, no Streamlit). Looks like a deliberate backend/event-driven-only
      design, not an oversight. Decide: stay backend-only (alerts via Logic
      Apps/Service Bus), or scope a dashboard?
