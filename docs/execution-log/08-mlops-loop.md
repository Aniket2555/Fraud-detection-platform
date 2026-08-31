# 08 — Phase 6: MLOps Loop (Drift, Retraining, Champion/Challenger, Shadow Scoring, Rollback)

**Starting state:** all Phase 6 code (drift detector, concept drift detector,
chargeback feedback notebook, KPI tracker, retraining pipeline, champion/
challenger gate, shadow scoring, rollback sentinel) already existed in the
repo — written but never run, same pattern as every earlier phase. The
plan's own "Known Issues" section had already caught and fixed a few bugs
via code review, but most of what's below was found by actually running
the code against real data for the first time.

**End state:** the full MLOps loop verified end-to-end against real
infrastructure: real label reconciliation, a real KPI tracking table, real
drift detection (both stable and correctly-triggered-critical cases), a
real full-ensemble retrain (XGBoost + Autoencoder + Isolation Forest +
calibrators + meta-learner, Optuna-tuned), a real 4-gate Champion/
Challenger evaluation with McNemar's significance that promoted a new
model version, real shadow scoring logs, and a real rollback that reverted
a promoted model after a simulated production anomaly.

## Two environment gaps had to be fixed before any of this could run

1. **The Databricks CLI's saved auth config was gone** (a fresh
   environment, not the one Phases 1-5 used). Fixed by setting
   `DATABRICKS_HOST` and letting the CLI fall back to Azure CLI
   federation (`databricks auth` picks up the already-logged-in `az`
   session automatically) — no new token needed.
2. **The Command Execution API's Python context has none of the ML stack
   pre-installed** (`mlflow`, `xgboost`, `torch`, `optuna`,
   `databricks-sdk` were all missing) — Phase 4's original interactive
   training must have used `%pip install` inside a notebook session that
   no longer exists. Re-installing via `%pip install` inside a fresh
   context hit two further snags, both fixed once, in this order:
   - `ImportError: cannot import name 'Sentinel' from 'typing_extensions'`
     — the base runtime's older `typing_extensions` was shadowing the
     newly-installed one on `sys.path`. Fixed by reordering `sys.path` to
     put the ephemeral pip env first and force-reimporting the module.
   - A **stale wheel**: the cluster's installed `fraud_detection` package
     (built earlier, cluster-library-installed) didn't reflect the
     current repo — still had the old calendar-month train/val/test split
     instead of the chronological one. Rebuilt the wheel locally
     (`python -m build --wheel`), uploaded it as a workspace file (DBFS
     root is disabled on this workspace — same restriction noted in
     Phase 3 — so `/Workspace/...` paths were used instead), and
     `%pip install --force-reinstall` it into the session, purging
     `sys.modules` entries first so the fresh code actually loads.

## Building the bridge tables Phase 6 assumes exist

Several tables Phase 6's notebooks read from were never produced by any
earlier phase — this is the same "gap between the plan and what actually
got built" pattern seen in every prior phase, just concentrated here
because Phase 6 is the first to read cross-phase.

| Table | Why it didn't exist | Fix |
|---|---|---|
| `gold.decision_audit_log` | Phase 5's `audit_logger` writes JSON files to ADLS, not a Delta table | New notebook `ingest_decision_audit_log.py` reads the JSON files and materializes the table (renaming `fraud_score` → `fraud_probability` to match every downstream consumer) |
| `gold.analyst_decisions_sync` | Never synced from the Phase 5 Azure SQL case DB | New notebook `sync_analyst_decisions.py`, reading via JDBC (see below) |
| `bronze.chargeback_reports` | No real payment processor in this dev environment | New notebook `generate_synthetic_chargebacks.py`, clearly marked synthetic (`_is_synthetic=true`) |
| `gold.reconciled_labeled_transactions` | Never created — `MERGE INTO` needs an existing target | Added `CREATE TABLE IF NOT EXISTS` to `ingest_chargeback_feedback.py` |
| `gold.model_performance_kpis`, `gold.drift_monitoring_history`, `gold.shadow_scoring_logs` | Same — `MERGE`/`append` targets never created | Same fix, one `CREATE TABLE IF NOT EXISTS` per table in its respective notebook |
| `gold.train_feature_snapshot`, `gold.train_prediction_baseline` | Nothing ever snapshotted a training-time reference distribution | New notebook `snapshot_training_baseline.py` |

### The `gold` container had no Unity Catalog external location

Reading the Phase 5 audit JSON files directly via `abfss://gold@...`
failed with `Invalid configuration value detected for fs.azure.account.key`
— Phase 3 only ever registered external locations for `raw`/
`checkpoints`/`quarantine` (`gold` tables were always written via
`saveAsTable()`, never read by raw path). Fixed with one `CREATE EXTERNAL
LOCATION` (reusing the existing `stfraudlakedev-credential` storage
credential) — a manual SQL step, matching how the original three
locations were set up (no Terraform manages this).

### Azure SQL JDBC access from Databricks

The `azure-sql-conn-str` Key Vault secret Phase 5 created was left as a
placeholder value, never actually populated. Filled in a proper JDBC
connection string as a new secret (`azure-sql-jdbc-url`) so
`sync_analyst_decisions.py` could read `fraud_cases`/`analyst_decisions`
directly.

## Testing the label reconciliation priority logic for real

`ingest_chargeback_feedback.py` referenced `silver.transactions` (the
Phase 1 static IEEE-CIS batch table — no `customer_id`/`card_id` columns
at all) instead of `silver.streaming_transactions`, the same class of bug
already fixed once in `ml/training/data_preparation.py`. Fixed the same
way. Also missing `merchant_id`/`latitude`/`longitude` from its output
schema — added once the retraining feature-schema mismatch below made the
gap obvious.

To actually prove the label-priority logic (chargeback overrides an
earlier analyst "confirmed legit" decision) rather than just running it
once, tested incrementally across two real MERGE passes:
1. Inserted a synthetic `fraud_cases`/`analyst_decisions` row (via
   `sp_upsert_fraud_case` + a manual insert) for a real streaming
   transaction, marked `CONFIRMED_LEGIT` — ran reconciliation — confirmed
   `label_source=ANALYST_CONFIRMED_LEGIT, confidence=0.9`.
2. Seeded a chargeback for the *same* transaction — ran reconciliation
   again — confirmed the **same row**, via a genuine `MERGE ... WHEN
   MATCHED AND source.label_confidence > target.label_confidence`
   update, flipped to `label_source=CHARGEBACK, confidence=1.0,
   is_fraud_reconciled=1`.
3. Ran reconciliation a third time, unchanged — row count stayed at
   31,265 both before and after, confirming true MERGE idempotency (no
   duplicates from re-running).

## Unity Catalog registry, not the legacy workspace registry

Every piece of Phase 6 code (`champion_challenger_gate.py`,
`shadow_scoring_batch.py`, `automated_rollback_sentinel.py`) used the
legacy MLflow Model Registry API (`models:/name/Stage`,
`transition_model_version_stage`, `get_latest_versions(stages=...)`).
This workspace's registry is Unity Catalog (3-level names, no stages) —
the exact same registry Phase 4 already had to adapt to for the baseline
model. Rewrote all three to use `fraud_detection_dev.gold.
fraud_ensemble_champion` with `@champion`/`@challenger` **aliases**
instead of stages. A rejected challenger is now tagged `@challenger`
(inspectable) rather than silently discarded. The rollback sentinel's
"find the previous version" logic (already fixed once in the plan's own
Known Issues list, for the legacy registry's `get_latest_versions`
quirk) was rewritten again for UC: since UC `ModelVersion` objects carry
no stage/timestamp-of-demotion, "previous version" is determined by
version number (this pipeline only ever creates one new version per
retrain and promotes it or leaves the existing one, so "highest version
number below the current champion" is equivalent to "was champion
immediately before this one").

**Registered the very first Champion** (never done before this session):
found Phase 4's leftover `/tmp/ensemble_artifacts` still on the driver's
local disk (the cluster hadn't restarted since), re-logged them as a real
MLflow model *with a signature* (the original Phase 4 test run never
logged one — UC rejects unsigned models outright), and registered it as
`fraud_detection_dev.gold.fraud_ensemble_champion` version 1, `@champion`.

## The feature-schema mismatch that broke every cross-model comparison

The single most consequential bug found this session, in three files at
once. Every one of `retrain_pipeline.py`, `champion_challenger_gate.py`,
and the new `snapshot_training_baseline.py` computed
`feature_cols` the same way the plan specified:
```python
feature_cols = [c for c in df.columns if c.startswith((
    "feature_", "vel_", "geo_", "base_", "merch_", "graph_"
))]
```
But the actually-registered Champion's real training schema (recovered
from its `feature_names.json`) was **30 columns**: `["amount",
"latitude", "longitude"]` plus the prefix-matched engineered features,
**minus** `base_cust_first_seen_ts`/`base_cust_last_seen_ts` (two raw
timestamp columns the naive prefix match swept in, which
`FraudEnsemblePyFunc` can't cast to float — `base_cust_tenure_days`
already captures that information numerically).

The prefix-only version silently produced a **3-column** feature set
(just `amount`/`latitude`/`longitude`, since `gold.
reconciled_labeled_transactions` never carried the engineered PIT-joined
columns at all — those exist only in each script's own in-memory
`enriched_df`, never written back to the table). This didn't fail
quietly — `champion_challenger_gate.py` crashed outright the first time
it tried to score anything: `Shape of input (1, 3) does not match
expected shape (-1, 30)`.

Fixed by defining the feature set identically (raw columns + prefix
match minus the two timestamp columns) in all three files, and by adding
the same `multi_entity_pit_join` call `retrain_pipeline.py` already used
to `champion_challenger_gate.py` and `shadow_scoring_batch.py` — neither
had ever actually joined the engineered features onto their evaluation
sets before scoring.

## Other real bugs found by running the code

| # | File | Bug | Fix |
|---|---|---|---|
| 1 | `retrain_pipeline.py` | `os.makedirs("/dbfs/tmp/...")` → `OSError: Operation not supported` — the DBFS FUSE mount is disabled on this workspace (same root cause as Phase 3's GraphFrames checkpoint-dir issue) | Switched to local driver disk (`/tmp/...`) — fine since the cluster is single-node |
| 2 | `retrain_pipeline.py` | `sklearn`/`torch` components (`IsolationForest`, the autoencoder) don't accept NaN natively, unlike XGBoost — `geo_dist_km` is null for a card's first-ever transaction, any PIT-joined feature can be null on a missed lookback window; training had no NaN guard even though `FraudEnsemblePyFunc` already has one at inference time | Added `np.nan_to_num(...)` on `X_train`/`X_val`/`X_test`, matching the inference-time guard |
| 3 | `retrain_pipeline.py` | `mlflow.pyfunc.log_model(artifact_path=..., ...)` with no `signature=` — UC rejects unsigned model versions outright at `champion_challenger_gate.py`'s `create_model_version()` call, not at logging time (fails much later, after a full ensemble has already trained) | Build a real signature from one scored row (`infer_signature`) at log time; switched `artifact_path=` → `name=` (current MLflow API) |
| 4 | `retrain_pipeline.py`, `champion_challenger_gate.py` | NaN guard cast to `float64`, but champion/challenger evaluation code builds arrays as `float32` — the resulting model's inferred signature required `float64`, so every real scoring call from `champion_challenger_gate.py` failed with a dtype mismatch | Standardized on `float32` everywhere, matching `FraudEnsemblePyFunc._validate_and_coerce`'s own internal cast |
| 5 | `shadow_scoring_batch.py` | The `pandas_udf` Iterator pattern was wrong for multi-column input: `score_champion_udf(*[col(c) for c in champion_features])` passes 30 columns, but the function signature only handled the single-column case (`for val in batch_series`) — crashed immediately: `takes 1 positional argument but 30 were given` | Replaced the whole pandas_udf/broadcast approach with the same reliable `toPandas()` + per-row-loop pattern already used in `champion_challenger_gate.py` (no vectorization was being gained anyway — `FraudEnsemblePyFunc` has no true batch `predict()` path, and the cluster is single-node) |
| 6 | `shadow_scoring_batch.py` | `.select(["...", "amount", "..."] + list(set(champion_features + challenger_features)))` selected `amount` twice (once explicitly, once via `RAW_FEATURE_COLS` inside `champion_features`) → `AMBIGUOUS_REFERENCE` at the `MERGE INTO` | Removed the redundant explicit `"amount"` |
| 7 | `champion_challenger_gate.py`, `automated_rollback_sentinel.py` | `scipy.stats.binom_test` was removed in the installed scipy version (replaced by `binomtest`, returning a result object) | Switched to `binomtest(...).pvalue` |

## Performance: capped, not vectorized

`FraudEnsemblePyFunc.predict()` has no true batch path — every gate/
shadow-scoring evaluation scores one row at a time in a Python loop.
Against the full 14-day eligible set (tens of thousands of rows), a first
attempt at `champion_challenger_gate.py` ran for 7+ minutes with no end
in sight; cancelled it via the Command Execution API's `/commands/cancel`
endpoint and capped both `champion_challenger_gate.py` (`EVAL_SAMPLE_SIZE
= 2000`) and `shadow_scoring_batch.py` (`SHADOW_SAMPLE_SIZE = 2000`) to a
random sample — still statistically meaningful for PR-AUC/recall/FPR
estimation, and finishes in under a minute.

## Sparse real label data — a genuine, honest limitation

This dev environment has exactly **one** real reconciled label (the
synthetic chargeback seeded to test priority ordering above).
`retrain_pipeline.py`'s own data-quality guards (`>= 1000` labeled rows,
fraud rate between 0.1%-30%) correctly refused to train on that — this
is the guard working as designed, not a bug. To actually exercise the
retraining code path, synthetically matured a random sample of
currently-`MATURING` rows (reusing each row's own original streaming-
replay `is_fraud` flag as the synthetic "chargeback outcome", so the
resulting fraud rate is realistic rather than hand-picked), clearly
tagged `label_source='SYNTHETIC_TEST_MATURED'`. This is a genuine
characteristic of a Free-Trial sandbox with no real payment processor —
not something any amount of code fixing resolves.

## Verifying against the Phase 6 checklist (§6.13 of the plan)

| # | Check | Result |
|---|---|---|
| 1 | Label reconciliation, MERGE idempotency | Ran 3x; 31,265 rows before and after, no duplicates |
| 2 | Label confidence priority ordering | `ANALYST_CONFIRMED_LEGIT`(0.9) → `CHARGEBACK`(1.0) via a genuine incremental MERGE across two separate runs |
| 3 | KPI tracking table populated | `model_performance_kpis` has real TP/FP/FN/TN, precision=1.0, recall=1.0 for the one matched decision+label pair |
| 4 | PSI/KS/JSD accuracy | Local unit tests against known-identical and known-3σ-shifted distributions — all correct |
| 5 | Concept drift detector fires | `CONCEPT_DRIFT_DETECTED` correctly returned on a synthetic distribution shift |
| 6 | Daily drift job orchestration | Ran feedback → KPIs → drift → (shadow scoring separately) manually via the Command Execution API; also deployed all 10 notebooks to `/Shared/fraud-detection/mlops/` as real workspace notebooks and created a real (paused) Databricks Workflow job (`mlops_daily_drift_and_retrain`, job_id `1103091557487533`) so `dbutils.notebook.run()` auto-triggers actually resolve |
| 7 | Retraining produces a challenger | Full ensemble trained twice for real (Optuna 30 trials); best trial PR-AUC 0.8753, challenger test PR-AUC 0.6364 (first run, before promotion) |
| 8 | All 4 gates evaluated | PR-AUC, High-Value Recall (no high-value txns in sample, pass-by-default), FPR cap, Latency (p99=30.94ms) — all logged to the challenger's MLflow run |
| 9 | McNemar's significance | p=0.0000 (b=0, c=64) — challenger strictly dominated champion on the eval sample |
| 10 | Model promoted | All 4 gates passed → version 2 promoted to `@champion` for real |
| 11 | Shadow scoring logs populated | 2,055 rows, 0 scoring errors, real champion/challenger score deltas |
| 12 | Rollback sentinel | Healthy-state run correctly reported no anomaly (real baseline comparison, not a vacuous pass); a seeded synthetic anomaly (FPR 0.31 vs baseline 0.01, recall 0.19 vs baseline 0.80) correctly triggered a real rollback, reverting `@champion` from v2 back to v1 |

## What's not done / cleaned up

- The rollback test's synthetic KPI rows were deleted afterward; `@champion`
  was restored to version 2 (the legitimately gate-passed model) after
  confirming rollback worked. `@challenger` alias was removed (was
  temporarily pointed at v1 to give `shadow_scoring_batch.py` something
  to compare against).
- The `SYNTHETIC_TEST_MATURED` label-source rows in
  `gold.reconciled_labeled_transactions` were **not** removed — they're
  what version 2 of the Champion was actually trained on, so deleting
  them would orphan that model's provenance. Left in place, clearly
  tagged.
- The Databricks Workflow job (`mlops_daily_drift_and_retrain`) was
  created but left **paused** — it was not started running on its daily
  6am IST schedule. Starting it (or running it once on-demand via
  `databricks jobs run-now`) is a deliberate choice left to the user,
  not something this session enabled silently.
- Application Insights still isn't wired in (same gap noted in Phase 5) —
  MLflow's own run/experiment UI was the primary way to inspect what
  happened here.

## To reproduce this from scratch

1. `export DATABRICKS_HOST=...` and rely on `az login`'s existing Azure
   CLI credentials for Databricks auth (no PAT needed).
2. Rebuild and deploy the `fraud_detection` wheel if the cluster's copy
   is stale (`python -m build --wheel`, upload as a workspace file,
   `%pip install --force-reinstall --no-deps <path>`, purge
   `sys.modules` entries for `fraud_detection.*`).
3. Create the `gold` external location (one-time SQL command) and
   populate the `azure-sql-jdbc-url` secret if not already done.
4. Run, in order: `generate_synthetic_chargebacks.py`,
   `sync_analyst_decisions.py`, `ingest_chargeback_feedback.py`,
   `ingest_decision_audit_log.py`, `track_model_performance_kpis.py`.
5. Register an initial Champion if none exists (see the champion
   registration approach above — needs a signature).
6. Run `snapshot_training_baseline.py`, then `run_daily_drift_check.py`.
7. Run `retrain_pipeline.py` (needs >=1000 labeled rows at
   confidence>=0.7 with a 0.1%-30% fraud rate — synthesize maturation
   data if the real labeled set is too sparse, as done here).
8. Run `champion_challenger_gate.py` with the printed `challenger_run_id`.
9. Run `shadow_scoring_batch.py` (needs a `@challenger` alias set on
   some version, even a reused older one, to have anything to compare).
10. Run `automated_rollback_sentinel.py` — seed a synthetic baseline KPI
    row (30-day-old) for a real healthy-path test, and a synthetic
    anomaly row to confirm the rollback path.
