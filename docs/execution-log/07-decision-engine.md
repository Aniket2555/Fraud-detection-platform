# 07 — Phase 5: Decision Engine, Case Management, Compliance Audit

**Starting state:** Phase 5's Terraform modules (`service-bus`, `azure-sql`,
`app-configuration`) and application code (3 Azure Functions, 5 SQL
migrations, 2 stored procedures, 2 Logic App workflow JSON files) already
existed in the repo from an earlier design/code-review pass — but nothing
had ever been deployed or run. The plan document's own "Known Issues"
section already listed a set of bugs found by code review and fixed in
source; this session is the first time any of it touched real Azure
infrastructure.

**End state:** Service Bus topic + 3 subscriptions, Azure SQL Serverless
with all 5 tables + 2 stored procedures, App Configuration with dynamic
thresholds, and all 3 Azure Functions (Consumption plan) deployed and
verified end-to-end against real infrastructure. One previously-undetected
production bug was found and fixed (see below). Logic Apps were **not**
deployed this session — see "What's not done."

## What was already live vs. newly deployed

The Service Bus namespace, Azure SQL server/database, and App
Configuration store were actually already provisioned during
`02-infrastructure.md`'s original `terraform apply` (they were wired into
`infrastructure/main.tf` from the start). This session's infra work was:

1. Running the 5 SQL migrations + 2 stored procedures for real (nothing
   had created the tables yet)
2. Building brand-new Terraform for Function App hosting (didn't exist
   anywhere — the plan's own file tree never included it)
3. Deploying the 3 Functions' code

## New Terraform: Function App hosting

No module provisioned the actual compute to run
`functions/decision_engine`, `functions/audit_logger`,
`functions/dlq_monitor` — the plan's file tree only ever specified the
function *code*. Added `infrastructure/modules/function-app/`:
- One shared Storage Account (Consumption-plan Functions need one for
  trigger/lease state; shared across all 3 to minimize footprint)
- One Linux Consumption Service Plan (`Y1` — free for the first 1M
  executions/month)
- 3 `azurerm_linux_function_app` resources, each with a system-assigned
  managed identity
- A `Storage Blob Data Contributor` role assignment scoping
  `audit_logger`'s identity to the data lake (it writes compliance
  records via `ManagedIdentityCredential`, not a connection string)

Also added a `primary_read_connection_string` output to the
`app-configuration` module (App Config didn't previously expose one) and
a `dlq_replay_connection_string` output to `service-bus` (send+listen,
no manage — narrower than the namespace root key) for the DLQ replay
tooling.

```bash
cd infrastructure
terraform plan -input=false -var-file=environments/dev.tfvars -out=dev.tfplan
terraform apply -input=false dev.tfplan
```

## Running the SQL migrations

```bash
# Firewall: serverless SQL only allows Azure services + explicitly
# allow-listed IPs by default. Temporarily added, removed when done:
az sql server firewall-rule create --resource-group rg-fraud-detection-dev \
  --server sql-fraud-dev --name AllowMyDevIP \
  --start-ip-address <your-ip> --end-ip-address <your-ip>

pip install pymssql   # bundles FreeTDS -- no separate ODBC driver install needed on Windows
```

Ran `database/migrations/V001` through `V005`, then
`database/stored_procedures/sp_upsert_fraud_case.sql` and
`sp_update_case_status.sql`, via a small pymssql runner script (splits
each file on `GO` batch separators, executes sequentially).

**Serverless auto-pause gotcha:** the first connection attempt failed
with SQL error 40613 ("database is not currently available... resuming")
— `GP_S_Gen5_1` had auto-paused from zero activity. Retried with
exponential backoff; resumed within ~45 seconds.

### Verifying the SQL layer for real (not just "tables exist")

Directly exercised both stored procedures against the live database:
- Called `sp_upsert_fraud_case` twice for the same `transaction_id` with
  different scores — confirmed identical `case_id` returned both times,
  first call logged `CASE_CREATED` in `case_events`, second logged
  `MODEL_SCORE_UPDATED`, and `fraud_cases` reflected the second call's
  values (idempotent MERGE + full field refresh on match, both correct)
- Called `sp_update_case_status` with a note containing `"quotes"` and a
  `\backslash` — confirmed the resulting `event_data` JSON parses
  correctly (the `STRING_ESCAPE` fix works)
- Attempted an insert with an invalid `decision_action` value — confirmed
  the `CHECK` constraint correctly rejects it

## Deploying the 3 Azure Functions

This took several iterations to get right on Windows:

1. **`az functionapp deployment source config-zip` doesn't build Python
   dependencies.** On current-generation Linux Consumption Function Apps,
   this command uploads the zip straight to blob storage and points
   `WEBSITE_RUN_FROM_PACKAGE` at it — it does **not** invoke Oryx/Kudu to
   run `pip install -r requirements.txt`, even with
   `SCM_DO_BUILD_DURING_DEPLOYMENT=true` set. Functions with third-party
   imports (`azure-servicebus`, `azure-identity`, etc.) failed to index
   at all.
2. **Fix:** pre-installed dependencies into the deployment package
   manually, targeting the Linux runtime rather than the local Windows
   Python:
   ```bash
   pip install --platform manylinux2014_x86_64 --only-binary=:all: \
     --python-version 3.11 \
     --target build_dir/.python_packages/lib/site-packages \
     -r functions/<name>/requirements.txt
   ```
   then zipped `build_dir` (source + `.python_packages/`) and deployed
   that. `azure-functions-core-tools` (the normal path for this) couldn't
   be installed via `npm` in this environment (pre-existing, unrelated
   npm auth issue) — this was the workaround.
3. **`az functionapp function list` is unreliable immediately after
   deploy** — it reads a cached ARM view that lags the real function
   host by a minute or more. Verified indexing instead via the function
   app's own admin API (`GET /admin/functions?code=<masterKey>`), which
   reflects the live host state immediately.

```bash
# Per function:
az functionapp deployment source config-zip \
  --resource-group rg-fraud-detection-dev --name <func-app-name> \
  --src <built-zip-with-dependencies>
```

## The real bug: audit_logger wrote 0-byte compliance records

This is the one genuine production defect found this session (not
already flagged in the plan's own code-review pass).

**Symptom:** `decision_engine` correctly published events for every
non-approve decision; `audit_logger`'s Service Bus trigger correctly
fired; but every single delivery attempt failed, and after
`max_delivery_count` (10) retries, every message dead-lettered with
`MaxDeliveryCountExceeded`. The handful of files that did land in
`gold/audit_logs/` were all exactly **0 bytes**.

**Root cause:** `_persist_audit_record()` called
`dir_client.create_file(file_name)` (which creates an empty file on ADLS
Gen2) and then `file_client.upload_data(record_bytes, overwrite=False)`
on that same, now-already-existing file. Confirmed by reproducing the
exact write logic locally against the real storage account (using
`AzureCliCredential` in place of the deployed `ManagedIdentityCredential`,
same account, same code path):
- `create_file()` then `upload_data(overwrite=False)` → the file "already
  exists" (from the create moment before), write refused, 0 bytes remain
- Removing the `create_file()` call and using `get_file_client()` instead
  (no server-side creation) → `upload_data(overwrite=False)` internally
  issues an `append_data` call assuming the path already exists, and
  fails with `ResourceNotFoundError: PathNotFound` since it doesn't

**Fix:** explicit existence check before writing, rather than relying on
`upload_data(overwrite=...)` to double as both "create if absent" and a
dedup signal:
```python
file_client = dir_client.get_file_client(file_name)
try:
    file_client.get_file_properties()
    already_exists = True          # duplicate delivery -- skip
except ResourceNotFoundError:
    already_exists = False
if not already_exists:
    file_client.upload_data(record_bytes, overwrite=True)
```
Verified against real Service Bus traffic end-to-end after the fix: a
fresh decision-engine call produced a real 514-byte JSON audit record
with all expected fields (including the dynamic `threshold_config` used
for that decision), consumed cleanly with zero dead-letters.

## Replaying the stale dead-lettered messages

6 messages had accumulated in `sub-audit-log`'s DLQ from testing against
the broken code. Used `scripts/dlq_replay_handler.py` (already fixed for
the `ServiceBusSubQueue.DEAD_LETTER` API per the plan's own known-issues
list) to re-publish them:
```bash
export SERVICE_BUS_CONN_STR="<dlq-replay-rule connection string>"
python scripts/dlq_replay_handler.py sub-audit-log
```
All 6 replayed messages were consumed cleanly by the fixed `audit_logger`
— confirmed via file listing (500+ bytes each, not 0).

## Verifying the Decision Engine against the checklist (§5.12 of the plan)

| # | Check | Result |
|---|---|---|
| 7 | Decision Function < 15ms | Warm calls: 0.14–0.2ms internal latency. First call after a threshold-cache refresh: ~85ms (App Config round-trip). Cold container start (platform-level, not code): several hundred ms — outside the Function's own control. |
| 8 | Score < 0.10 approves | `fraud_probability=0.05` → `200`, `approve`, no Service Bus publish |
| 9 | Score > 0.90 blocks & publishes | `fraud_probability=0.95` → `403`, `block`, event published and consumed downstream |
| 10 | Dynamic threshold change, no redeploy | Changed `FraudEngine:StepUpMaxThreshold` from `0.60` to `0.50` via `az appconfig kv set` — a call with `fraud_probability=0.55` correctly flipped from `step_up` to `manual_review` on the very next request, with zero code deployment. Reverted after confirming. |
| 11 | Compliance audit logger fires | Confirmed working end-to-end after the bugfix above |
| 12 | DLQ monitor detects dead-lettered messages | Manually dead-lettered a message, invoked `dlq_monitor` via its admin API (`POST /admin/functions/dlq_monitor`) — returned `202 Accepted`, and the DLQ message count was unchanged afterward (consistent with its documented peek-only behavior, which intentionally does not consume DLQ messages). **Could not directly verify the log content** — Linux Consumption Python apps don't expose classic Kudu log streaming/`LogFiles` VFS without an Application Insights resource wired in, which wasn't provisioned this session. |

## What's not done

- **Application Insights** was never provisioned — the Functions run
  without centralized log aggregation. Structured `logging.info()` calls
  exist throughout the code but currently only reach the platform's
  ephemeral console capture, not a queryable store.
- **Logic Apps** (`logic-apps/workflows/workflow_stepup_auth.json`,
  `workflow_case_management.json`) were **not deployed**. No Terraform
  module provisions `azurerm_logic_app_workflow` or the Service Bus/SQL
  API connections they depend on — this was already an open gap flagged
  in the plan document itself before this session started, and it
  remains open. `sub-stepup-auth` and `sub-case-mgmt` currently have no
  live consumer; messages will simply accumulate as active (undelivered)
  until either a Logic App or a consuming Function is built and deployed.
- The full OTP/analyst-assignment workflow described in the plan's
  mermaid diagram (§5.8) was never implemented anywhere in the repo, by
  the plan's own admission — this remains a genuine follow-up project,
  not something this session addressed.
- Case creation for `manual_review` decisions (via `sub-case-mgmt`) is
  therefore not yet wired end-to-end in a live/deployed sense, even
  though the SQL layer it would call (`sp_upsert_fraud_case`) is fully
  verified and working.

## To reproduce this from scratch

1. Ensure `terraform apply` from `02-infrastructure.md` has run (creates
   Service Bus, Azure SQL, App Configuration, and now also the 3
   Function Apps + their hosting plan).
2. Add a temporary SQL firewall rule for your IP, run the 5 migrations +
   2 stored procedures via pymssql (or any SQL client), remove the
   firewall rule when done.
3. For each of the 3 functions: `pip install --platform
   manylinux2014_x86_64 --only-binary=:all: --python-version 3.11
   --target <dir>/.python_packages/lib/site-packages -r
   functions/<name>/requirements.txt`, zip the function directory
   (source + `.python_packages/`), deploy with `az functionapp
   deployment source config-zip`.
4. Verify each function is indexed via `GET
   https://<app>.azurewebsites.net/admin/functions?code=<masterKey>`
   (not the CLI's `function list`, which lags).
5. Test the decision engine with `POST /api/evaluate-decision` across all
   4 score bands; confirm Service Bus subscription message counts change
   accordingly (`az servicebus topic subscription show ... --query
   countDetails`).
6. Change a threshold via `az appconfig kv set` and confirm a boundary
   probability reclassifies within ~60 seconds (the cache TTL), no
   redeploy.
