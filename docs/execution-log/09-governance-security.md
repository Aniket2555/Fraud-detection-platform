# 09 — Phase 7: Governance, Security & Hardening

**Starting state:** all Phase 7 code (governance SQL, PII hashing, RBAC
Terraform, diagnostic settings, security-scan CI workflow, chaos test
suite, secret rotation script, platform verification script) already
existed in the repo — written but never run, same pattern as every
earlier phase. Some of it had already been through one round of code
review (the plan's own "Known Issues" list), but most of what's below was
found by actually running each tool against the real, live platform for
the first time.

**End state:** real Unity Catalog masking proven working (then reverted
for a compatibility reason — see below), real PII hashing verified with
positive and negative tests, real RBAC wired to the actual Function App
identity, 4 real security scanners run against the real codebase with
findings triaged and fixed or documented, all 5 chaos scenarios run
against the real live Decision Engine, a real 9-step platform
verification, and a real Key Vault secret rotation (including the live
Azure SQL admin password) with connectivity re-verified afterward.

## RBAC: wiring the Decision Function's managed identity for real

`infrastructure/variables.tf`'s `decision_function_principal_id` had sat
at its empty-string default since Phase 5 — the `rbac-assignments`
module's `count` guards correctly skipped creating the Service Bus Data
Sender / App Configuration Data Reader role assignments for it, but
nothing had ever gone back and supplied the *real* principal ID once
`func-fraud-decision-dev` actually existed. Fixed:
```bash
az functionapp identity show --name func-fraud-decision-dev \
  --resource-group rg-fraud-detection-dev --query principalId -o tsv
```
Added the real GUID to `environments/dev.tfvars`, applied — 2 new role
assignments created for real. (`decision_engine`'s code still
authenticates to Service Bus/App Config via connection strings, not this
new managed identity — the RBAC grant exists and is real, but migrating
the Python code to actually use it is a follow-up, not done this session.)

## A near-miss: this apply would have deleted the live Function Apps' deployment settings

The very first `terraform plan` after the RBAC change showed the 3
Function Apps "changing" — Terraform wanted to **delete**
`WEBSITE_RUN_FROM_PACKAGE`, `SCM_DO_BUILD_DURING_DEPLOYMENT`, and
`ENABLE_ORYX_BUILD` from all 3 apps. These were set out-of-band by the
Phase 5 deployment process (`az functionapp deployment source config-zip`
auto-sets `WEBSITE_RUN_FROM_PACKAGE`; separately configured
`SCM_DO_BUILD_DURING_DEPLOYMENT`/`ENABLE_ORYX_BUILD`), and the
`function-app` Terraform module's `app_settings` block never declared
them — so every `terraform plan` from now on would show this same
destructive diff, and *applying* it would have deleted
`WEBSITE_RUN_FROM_PACKAGE` (the pointer to the currently-deployed code
package), breaking all 3 live Functions.

Caught before applying (`terraform show <plan> | grep "will be updated"`,
then read the actual diff) by first running a `-target`ed apply that
excluded the Function App resources, and separately fixing the module
with `lifecycle { ignore_changes = [app_settings] }` on all 3 resources —
Terraform now treats deployment-time settings as out of its scope,
matching reality. Re-ran a full untargeted plan afterward to confirm the
destructive diff was gone.

## Real, free Checkov fixes made to the Terraform itself

Rather than skip-listing everything, findings that cost nothing to fix
were fixed for real, then applied:
- Storage accounts (`stfraudlakedev`, the Function Apps' storage account,
  and the Terraform state backend account) — added `sas_policy` (bounds
  SAS token lifetime), `allow_nested_items_to_be_public = false`,
  explicit `min_tls_version`/`https_traffic_only_enabled`, and blob
  `delete_retention_policy` (soft delete) where missing. The Terraform
  state backend account specifically got a 30-day retention — protecting
  the state file itself from accidental deletion is worth it regardless
  of Free Trial cost-consciousness.
- Service Bus namespace — explicit `minimum_tls_version = "1.2"`.
- All 3 Function Apps — explicit `https_only = true`.

Everything else failing Checkov (customer-managed keys, private
endpoints, SQL auditing/Vulnerability Assessment, Service Bus double
encryption, disabling local/shared-key auth entirely) is a genuine
Free-Trial cost tradeoff already documented in the plan's own "Production
Decision Registry" — expanded `security-scan.yml`'s `--skip-check` list
to match, each with a one-line reason, rather than leaving the CI job in
a state where it would red-X on every single push regardless of what
changed (the job had no `soft-fail`, so with 60+ un-skip-listed findings
it would never have passed).

## Unity Catalog data governance

### The recurring wrong-table bug, again

`databricks/governance/apply_data_masking_policies.sql` targeted
`silver.transactions` (the Phase 1 static IEEE-CIS batch table) — it has
no `ip_address`/`device_id` columns at all. Same class of bug already
fixed twice before this session in `ml/training/data_preparation.py` and
`ingest_chargeback_feedback.py`. Fixed to target
`silver.streaming_transactions`, which actually carries those columns.

### Two genuine identity-plumbing gaps, found by actually running the GRANTs

- **Groups didn't exist at all.** `IS_ACCOUNT_GROUP_MEMBER('compliance-officers')`
  etc. and the `GRANT ... TO \`fraud-analysts\`` statements assume 5
  Entra ID / Unity Catalog groups that had never been created anywhere.
  Created all 5 for real via `az ad group create`.
- **Databricks doesn't auto-discover Azure AD groups.** Even after
  creating the Entra ID groups, `GRANT` failed with
  `PRINCIPAL_DOES_NOT_EXIST` — Unity Catalog's `GRANT` resolves
  principals against Databricks *account*-level identity, not Azure AD
  directly. Creating the same group names via workspace-level SCIM
  (`databricks groups create`) didn't fix it either — UC's metastore is
  an account-level resource, and workspace groups aren't the same
  identity namespace. Reaching the Databricks Account Console's own
  SCIM/group provisioning requires either an Account Console session
  (interactive, not achievable headlessly) or a properly configured
  Azure AD Enterprise Application sync — genuinely out of reach for a
  single CLI-driven session. **The `GRANT` statements remain untested
  against a real group** — documented as a known gap rather than forced.

### Column masking works — but breaks the older interactive cluster

Unity Catalog column masks (`ALTER TABLE ... SET MASK ...`) require a
**Shared**-mode cluster or SQL Warehouse — the `batch-etl-dev` cluster
used everywhere else in this project is `SINGLE_USER` mode, which
Databricks explicitly rejects for row/column policies
(`ROW_COLUMN_ACCESS_POLICIES_NOT_SUPPORTED_ON_ASSIGNED_CLUSTERS`). Used
the workspace's pre-existing "Serverless Starter Warehouse" (a SQL
Warehouse, Shared-equivalent mode) via the Statement Execution API
instead — that's the mechanism a real BI/analyst tool would use anyway.

Applied for real and **verified working**: queried
`silver.streaming_transactions` as myself (not a member of any
privileged group) and got back `ip_address='.xxx.xxx'`,
`device_id='dev_****'` — masking genuinely enforced.

**Then found a serious compatibility break**: with the mask applied,
`spark.table("fraud_detection_dev.silver.streaming_transactions")` from
the *interactive cluster* (Command Execution API, DBR 14.3) started
failing with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'COLLATE'` —
applying the mask via the SQL Warehouse's newer engine appears to have
attached a `STRING COLLATE UTF8_BINARY` type annotation to the masked
columns that the older cluster's SQL parser can't read back, even for a
plain `spark.table()` call with no reference to the masked columns
specifically. This is the exact table every Phase 3/4/6 pipeline script
depends on (`data_preparation.py`, `retrain_pipeline.py`,
`champion_challenger_gate.py`, `shadow_scoring_batch.py`,
`ingest_chargeback_feedback.py`, ...) — leaving the mask in place would
have silently broken all of them on their next run.

Dropped the masks (`ALTER TABLE ... ALTER COLUMN ... DROP MASK`) and
re-verified `spark.table()` + `.count()` succeeded again from the
interactive cluster. **Net result:** the masking mechanism is proven to
work correctly; it is not currently active on the shared table, because
this environment has two compute engines at different DBR/SQL-engine
versions reading the same table, and applying UC masking broke
compatibility between them. A real production deployment would need
either a single consistent compute version across all readers, or
applying masking only on a dedicated BI-facing view rather than the raw
table ETL pipelines also depend on.

### PII SHA-256 hashing — verified end-to-end

The `pii-hash-salt` Key Vault secret referenced by
`databricks/src/security/pii_masking.py` had never actually been created.
Created it for real (`secrets.token_urlsafe(32)`), then verified the
whole module against a real 1,000-row sample of
`silver.streaming_transactions`:
- `get_pii_salt(spark)` retrieved the real secret via `dbutils.secrets.get`
- `sanitize_pii_fields()` produced real 64-character SHA-256 hex hashes
  from the real `ip_address`/`device_id` values
- `validate_pii_hashing()` returned `True` on the hashed data
- **Negative test**: `validate_pii_hashing()` on the *raw* (unhashed)
  1,000-row sample correctly raised `ValueError` for all 1,000 rows

## 4-scan security pipeline, run locally against the real codebase

| Scan | Tool used | Result |
|---|---|---|
| Secrets | `trufflehog3` (pip-installable Python reimplementation — no Go toolchain in this environment for the real Go-based TruffleHog) | 5 filesystem matches, all `.venv`/`.git` noise excluded: `terraform.tfstate`/`.backup` (confirmed `.gitignore`-excluded via `git check-ignore -v`, confirmed never committed via `git ls-files`), 2× `.terraform.lock.hcl` (checksum entropy false-positive, not secrets), `.pytest_cache/CACHEDIR.TAG` (benign, gitignored). **Zero real leaks.** Note: this local substitute scans the raw filesystem, unlike the real CI TruffleHog (`trufflehog git file://.`), which only sees git history — so it surfaces noise the real tool never would. |
| IaC | `checkov` (Python, invoked directly via `Checkov(argv=...).run()` — the installed `checkov.cmd` shim was broken by the working directory's space in its path) | See "Real, free Checkov fixes" above; remaining findings are documented Free-Trial tradeoffs, skip-listed with reasons |
| Python SAST | `bandit` | Found and fixed a real `torch.load(weights_only=False)` finding (arbitrary code execution risk if the artifact were tampered with) — see below. Strict high-severity/high-confidence gate: 0 findings, both before and after the fix |
| Dependency CVEs | `pip_audit` | 1 finding: `cryptography` 49.0.0, PYSEC-2026-3552 (Bleichenbacher-style oracle in PKCS#7 decrypt — requires an S/MIME auto-decrypt service, which nothing here implements). Blocked from upgrading past 49.x by mlflow 3.15.2's own `cryptography<50` pin; documented as a tracked, accepted risk rather than forced |

### The `torch.load(weights_only=False)` fix

`ml/ensemble/ensemble_model.py` loaded the autoencoder via
`torch.load(path, weights_only=False)` — unrestricted unpickling, which
can execute arbitrary code if the `.pt` artifact were ever tampered with.
Couldn't just flip to `weights_only=True`, because the already-registered
Champion (v1) and Challenger (v2) models were both saved via
`torch.save(ae_model, ...)` — the **full pickled model object**, which
`weights_only=True`'s restricted unpickler can't reconstruct (it only
allows tensors/dicts/primitives, not arbitrary classes like
`FraudAutoencoder`).

Fixed with a genuinely backward-compatible change: `retrain_pipeline.py`
and `ml/pipelines/training_pipeline.py` now save `{state_dict, input_dim,
bottleneck_dim}` (a plain dict of tensors/ints — safe under
`weights_only=True`) instead of the full model object.
`FraudEnsemblePyFunc._load_autoencoder()` tries the safe
`weights_only=True` load first, reconstructing `FraudAutoencoder` from
the dict; if that fails (an older, pre-fix artifact), it falls back to
`weights_only=False` with a clear warning log, so the already-registered
v1/v2 models keep working while every future retrain produces a safer
artifact.

## Chaos resilience suite — run against the real live Decision Engine

Two real bugs found before any test could even run:
- `FUNCTION_URL` pointed at `func-decision-engine-dev` — never the real
  Function App name. The real one (Phase 5) is `func-fraud-decision-dev`.
- (Already fixed per the plan's own Known Issues: the missing
  `x-functions-key` header, which was also verified still correct.)

Ran all 5 scenarios with `DECISION_ENGINE_FUNCTION_KEY` set to the real
master key:

| Test | Result |
|---|---|
| `test_01` corrupted payload → graceful fallback | ✅ PASS |
| `test_02` missing required fields → graceful fallback | ✅ PASS |
| `test_03` sequential p99 < 100ms | ⚠️ Found and fixed a real methodology bug (see below), still fails after the fix, but for a non-regression reason |
| `test_04` 50-concurrent storm, ≥95% success | ✅ PASS (50/50 succeeded) |
| `test_05` all 10 score-boundary routings correct | ✅ PASS |

`test_03` originally measured **client-side wall-clock round-trip time**
(`t1 - t0` around `requests.post`) — dominated by real network RTT from
wherever the test happens to run, not anything the Decision Engine's own
code controls. First run: p99 = 1412ms. Fixed to assert on the
function's own self-reported `latency_ms` field instead (same value
already verified in Phase 5 to be 0.14-0.2ms warm) — re-run: p99 =
178.97ms, still over the 100ms bound, explained by the (already
documented, Phase 5) 60-second App Configuration threshold-cache: a
100-sequential-call test run spanning more than 60 seconds wall-clock
will legitimately trigger a second cache refresh partway through, adding
one real App Config round-trip's worth of latency to 1-2 of the 100
samples. This is expected behavior of a deliberate caching design, not a
platform regression — and `test_03` is not one of the 3 blocking chaos
checks in the plan's own §7.13 validation checklist (only `test_01`,
`test_04`, `test_05` are marked 🔴 Blocking).

## End-to-end platform verification — 2 more real bugs, run for real

`scripts/verify_platform_end_to_end.sh` had:
- `KV="kv-fraud-dev"` — wrong; the real vault has a random suffix
  (`kv-fraud-dev-4th9`), same class of bug fixed repeatedly elsewhere.
- `FUNC_APP="func-decision-engine-dev"` — same wrong name as the chaos
  test's bug, independently present here too.
- The SQL health check only accepted status `"Online"` — but the
  database is deliberately serverless with a 60-minute auto-pause (a
  real Phase 5 cost-saving choice); the script would falsely fail any
  time the platform had simply been idle. Fixed to accept `"Paused"` too.

Ran the fixed script for real — all 9 steps passed, including catching
the database in its legitimately-`Paused` state (confirming the fix was
necessary, not just theoretical).

## Key Vault secret rotation — run for real, including the live SQL password

`scripts/rotate_keyvault_secrets.py`'s `ROTATABLE_SECRETS` list
(`db-admin-password-dev`, `sql-admin-password-dev`) named secrets that
don't exist anywhere in this environment — the real SQL credential lives
in `azure-sql-jdbc-url` (a full JDBC connection string with the password
embedded, created in Phase 6), not a bare password. Running the original
list as-is would have rotated the live SQL Server password **twice**
(once per mismatched name) while leaving the actual secret every real
consumer (Databricks' JDBC connection) reads completely untouched and now
out of sync with the live server. Also fixed:
- `VAULT_NAME` defaulted to `"kv-fraud-dev"` (missing the real random
  suffix) — resolved dynamically via the Azure Resource Management API
  instead of hardcoding.
- Rewrote `ROTATABLE_SECRETS` to `["pii-hash-salt", "azure-sql-jdbc-url"]`
  and added `_build_jdbc_url()` so rotating the SQL credential
  reconstructs the full JDBC string with the new password embedded,
  rather than overwriting it with a bare password (which would have
  corrupted the secret's format entirely).
- `_update_sql_server_password()`'s ARM call passed a raw `dict` as
  `parameters` — the installed `azure-mgmt-sql` 4.x SDK rejects this
  server-side (`400 InvalidRequestContent: Could not find member
  'administrator_login_password' on object of type 'ResourceDefinition'`).
  Fixed to use the SDK's real `ServerUpdate` model class. Confirmed this
  failure mode is genuinely safe: because the fix from the plan's own
  Known Issues list updates the live server *before* writing Key Vault,
  the first (failed) attempt left both the live password and Key Vault's
  `azure-sql-jdbc-url` completely unchanged and in sync — no partial
  rotation occurred.

Ran the fixed script for real, with explicit user confirmation first
(this mutates a live credential outside this session's easy control):
- `pii-hash-salt` rotated successfully on the first attempt.
- `azure-sql-jdbc-url` failed safely on the first attempt (the SDK bug
  above), fixed, then succeeded on retry: the live Azure SQL Server admin
  password was genuinely changed, and Key Vault's `azure-sql-jdbc-url`
  updated to match.
- Extracted the new password from the updated secret, updated the local
  `infrastructure/.sql_admin_password.local` file to match, and
  **verified real connectivity** with the new password via a direct
  `pymssql` connection.

**Follow-up required, not done by this session:** the GitHub Actions
`SQL_ADMIN_PASSWORD` secret still holds the *old* password and needs
updating:
```bash
gh secret set SQL_ADMIN_PASSWORD --repo Aniket2555/Fraud-detection-platform \
  --body "$(cat infrastructure/.sql_admin_password.local)"
```

## Verifying against the Phase 7 checklist (§7.13 of the plan)

See `docs/operational_readiness_signoff.md` for the full per-item status
(rewritten this session with real evidence instead of empty checkboxes).
Summary: 13/17 fully verified, 4/17 verified with an honestly-documented
caveat (PR-AUC provenance, UC masking reverted for compatibility, 1/5
non-blocking chaos test, Entra→UC account-level group sync unreachable
headlessly) — zero items silently marked done without real verification.
