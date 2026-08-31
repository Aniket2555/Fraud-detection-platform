# 10 — Follow-up session: closing 3 previously-documented gaps

**Starting state:** the platform was fully deployed and verified (Phases
0-7), with three gaps left open on purpose and tracked in
`docs/operational_readiness_signoff.md`: Logic Apps designed but never
deployed, chaos `test_03` failing for a documented non-regression reason,
and Unity Catalog PII masking reverted for a compute-version compatibility
break. This session closed all three for real.

## 1. Logic Apps: deployed for the first time

`logic-apps/workflows/*.json` (Case Management, Step-Up Auth) existed as
plain JSON but no Terraform module ever provisioned them
(`infrastructure/variables.tf`'s own comment flagged this). Added
`infrastructure/modules/logic-apps/`, which reads both JSON files via
`jsondecode(file(...))` rather than duplicating their content into HCL, so
the deployed workflow always matches what's committed.

**Two real bugs found by actually deploying and triggering these:**

1. **`depends_on` can't be a dynamic expression.** `for_each`-created
   `azurerm_logic_app_action_custom` resources apply in parallel by
   default, but the Logic Apps API rejects an action whose `runAfter`
   target doesn't exist yet — `stepup_auth`'s 4-deep action chain
   (`Parse_Message_JSON` → `Upsert_Case_Record` → `Wait_For_Customer_Response`
   → `Escalate_If_Timeout`) failed with `InvalidTemplate` /
   `contains non-existent action`. Terraform's `depends_on` only accepts a
   fully static list — no `for` expressions, no `concat()` — so the fix
   was switching from `for_each` to one explicitly-named resource per
   action with a static `depends_on` chain (body still read dynamically
   from the JSON file, only the resource addressing is static).
2. **The workflow JSON's SQL dataset path used the wrong server
   identifier.** Both workflows call
   `/v2/datasets/@{encodeURIComponent('sql-fraud-dev')},.../procedures/...`
   — the *short* SQL Server name. The SQL managed connector's `v2/datasets`
   path segment is the actual TDS connection target, not a label, so
   `'sql-fraud-dev'` alone isn't a resolvable hostname. First live test
   failed with `Invalid connection settings / Not a valid data source`.
   Fixed both JSON files to use the FQDN
   (`sql-fraud-dev.database.windows.net`), matching the API connection's
   own `server` parameter.

**Also hit a real state/reality drift while doing this:** planning any
change that touched `module.azure_sql` showed Terraform wanting to reset
the live SQL admin password, because Phase 7's rotation
(`scripts/rotate_keyvault_secrets.py`) updated the server directly via the
ARM API, bypassing Terraform entirely — so Terraform's state still held
the pre-rotation password. Resolved by re-reading the current password
from Key Vault's `azure-sql-jdbc-url` secret into
`infrastructure/.sql_admin_password.local` before applying, so the apply
brought Terraform's state back in sync with reality instead of reverting
the live server to a stale credential. This drift will recur on every
future `terraform apply` for as long as password rotation happens outside
Terraform — worth keeping in mind, not something this session could fix
structurally.

**Verified end-to-end for real:** called the live
`POST /api/evaluate-decision` endpoint with a payload scored into the
`step_up` band, confirmed both workflows' `Upsert_Case_Record` action
completed with `code: OK` (the SQL managed connector round-trip only
returns that on a successful stored-procedure execution) on a fresh,
post-fix test message — not just on backlog messages that happened to
predate the fix.

## 2. Chaos `test_03`: fixed the real cause instead of loosening the test

`docs/execution-log/09-governance-security.md` documented `test_03`
(sequential p99 < 100ms) failing at p99=178.97ms for a "non-regression"
reason: `functions/decision_engine/function_app.py`'s `_get_thresholds()`
did a **synchronous, blocking** App Configuration call inline whenever its
60-second cache TTL expired, and a 100-sequential-request test run
legitimately spans more than 60 seconds of real network RTT — so 1-2 of
the 100 requests blocked on a live App Config round-trip mid-run.

That's a real latency bug, not just a test artifact: a hot request path
has no business blocking on a periodic config refresh just because it's
the unlucky request that noticed the cache was stale. Fixed with a
stale-while-revalidate pattern — `_get_thresholds()` now kicks off the
App Config refresh on a background daemon thread and immediately returns
the (about-to-be-updated) cached snapshot, instead of blocking the
request. Only the very first cold-start call (no cached value to serve
yet) still blocks synchronously.

Rebuilt the deployment package (`pip install --platform manylinux2014_x86_64
--only-binary=:all: --python-version 3.11 --target
.python_packages/lib/site-packages`, per `07-decision-engine.md`'s
recipe) and redeployed via `az functionapp deployment source config-zip`.

**Result, run against the live redeployed function:**

```
tests/chaos/test_resilience_scenarios.py::test_03_latency_sla_sequential
  Sequential p99 Latency (function-reported): 1.38 ms
  PASSED
```

All 5/5 chaos scenarios now pass (previously 4/5). `docs/operational_readiness_signoff.md`
updated accordingly.

## 3. Unity Catalog masking: re-enabled via a view, not a table-level mask

`09-governance-security.md` documented the original attempt: applying a
native column mask (`ALTER TABLE ... ALTER COLUMN ... SET MASK`) to
`silver.streaming_transactions` worked, but mutated the base table's
column type metadata with a `STRING COLLATE UTF8_BINARY` annotation that
the older DBR 14.3 interactive cluster's SQL parser can't read back —
breaking `spark.table()` for every Phase 3/4/6 pipeline script depending
on that table. The mask was dropped to restore pipeline functionality,
leaving masking verified-but-inactive.

**Fix:** `databricks/governance/apply_data_masking_policies.sql` now
creates `silver.streaming_transactions_masked` — a **view** that computes
the same `mask_ip_address()`/`mask_device_id()` functions at query time —
instead of altering the base table at all. A view is a separate object;
creating it doesn't touch the raw table's stored schema, so nothing that
calls `spark.table()` on the raw table is affected by the view's
existence. Applied for real via the SQL Warehouse (Statement Execution
API, `databricks-sdk`'s `WorkspaceClient.statement_execution`, since
column masks/views require Shared-mode compute the same as before).

**Verified both halves this time, not just the masking half:**

1. Queried the view via the SQL Warehouse as a non-privileged principal:
   `ip_address` came back `.xxx.xxx`, `device_id` came back `dev_****` —
   masking genuinely enforced, same result as the original (reverted)
   attempt.
2. Read the **raw table** from the DBR 14.3 interactive cluster (Command
   Execution API, the exact reproduction of the original break):
   `spark.table("fraud_detection_dev.silver.streaming_transactions").count()`
   → `31265`, no error. Confirmed the raw table is completely unaffected.
3. For completeness, also read the *view* (not just the table) from the
   same old interactive cluster — it fails with the identical
   `PARSE_SYNTAX_ERROR ... COLLATE` error the table used to. This is fine
   and expected: nothing in the pipeline needs to read the masked view
   from that cluster — it exists for analyst/BI consumption via the SQL
   Warehouse, which is the access pattern verified working above.

**Not fixed, same known gap as before:** every `GRANT ... TO
\`fraud-analysts\`` (and the other 4 role grants) still fails with
`PRINCIPAL_DOES_NOT_EXIST` — Unity Catalog resolves grant principals
against Databricks *account*-level identity, which still requires
Account Console-level SCIM configuration unreachable headlessly. The
masking mechanism itself doesn't depend on these grants and is proven
working; the grants remain untested against a real group, exactly as
before.

## Updated sign-off status

`docs/operational_readiness_signoff.md` item #15 (UC masking) and #16
(chaos suite) updated to reflect: masking is now genuinely active on
`silver.streaming_transactions_masked`, and 5/5 chaos scenarios pass. The
"Known, deliberately-undone items" list's Logic Apps entry removed (now
live); the Entra ID account-level group sync gap remains, unchanged.
