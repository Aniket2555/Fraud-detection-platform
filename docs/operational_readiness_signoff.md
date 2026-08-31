# Operational Readiness Sign-Off Checklist

Updated after actually running and verifying each check against real
deployed infrastructure (not a desk review) — see `docs/execution-log/`
for the full command-by-command record of every phase.

## Pre-Production Verification

| # | Category | Check | Owner | Status |
|---|---|---|---|---|
| 1 | Infrastructure | All Terraform modules apply successfully | Platform Team | ✅ Verified — `terraform plan` is clean except a known cosmetic `network_rules`/diagnostics drift (documented in `docs/execution-log/02-infrastructure.md`) |
| 2 | Infrastructure | Key Vault secrets populated and rotated | Platform Team | ✅ Verified — `pii-hash-salt` and the live SQL admin password (via `azure-sql-jdbc-url`) both rotated for real this session; connectivity re-verified with the new password (`docs/execution-log/09-governance-security.md`) |
| 3 | Data Pipeline | Bronze → Silver → Gold Medallion pipeline processes IEEE-CIS dataset | Data Engineering | ✅ Verified (`docs/execution-log/03-data-landing.md`) |
| 4 | Data Pipeline | Streaming pipeline ingests Event Hubs events and writes to Silver Delta | Data Engineering | ✅ Verified (`docs/execution-log/04-streaming.md`) |
| 5 | Feature Store | Features compute correctly with PIT join (zero temporal leakage) | Data Engineering | ✅ Verified (`docs/execution-log/05-feature-engineering.md`) |
| 6 | ML Model | Hybrid ensemble achieves PR-AUC ≥ 0.80 on test set | Data Science | ⚠️ Partially verified — the real Phase 6 retraining run achieved PR-AUC 0.8753 (Optuna best trial) on genuine (if partly synthetic-augmented) labeled data; the original Phase 4 baseline's PR-AUC of 1.0 was flagged at the time as a likely artifact of synthetic entity generation, not a validated production number (`docs/execution-log/06-model-ensemble.md`) |
| 7 | ML Model | Scoring endpoint responds < 100ms (p99) | Data Science | ✅ Verified — the ensemble's own internal scoring latency is well under budget; end-to-end Decision Engine latency (function-reported, not client wall-clock) is documented with one known caveat around periodic App Config cache refresh (`docs/execution-log/09-governance-security.md`, chaos test_03) |
| 8 | Decision Engine | 4 score bands route correctly (approve/step_up/manual_review/block) | Platform Team | ✅ Verified twice — once directly in Phase 5, again via the chaos suite's boundary-score test in Phase 7 |
| 9 | Decision Engine | Service Bus fan-out delivers to all 3 subscriptions | Platform Team | ✅ Verified (`docs/execution-log/07-decision-engine.md`) |
| 10 | Case Management | Azure SQL schema deployed (5 tables + 2 stored procedures) | Platform Team | ✅ Verified, including idempotent MERGE + audit trail behavior under real concurrent-style testing (`docs/execution-log/07-decision-engine.md`) |
| 11 | MLOps | Daily drift check runs and logs to Gold history table | Data Science | ✅ Verified, including a genuine `CRITICAL_DRIFT` detection (`docs/execution-log/08-mlops-loop.md`) |
| 12 | MLOps | Champion-Challenger gate evaluates all 4 metrics | Data Science | ✅ Verified — all 4 gates + McNemar's significance computed on a real challenger, resulting in a real promotion (`docs/execution-log/08-mlops-loop.md`) |
| 13 | Security | TruffleHog scan passes with zero verified leaks | Platform Team | ✅ Verified — 5 filesystem matches, all confirmed either `.gitignore`-excluded and never committed (`terraform.tfstate*`) or non-secret (lock file checksums, pytest cache marker); zero real leaks (`docs/execution-log/09-governance-security.md`) |
| 14 | Security | Checkov IaC scan passes with zero critical violations | Platform Team | ✅ Verified — real findings triaged: fixed for free where possible (storage soft-delete/SAS policy/TLS minimums, Function App HTTPS-only), the rest are documented Free-Trial cost tradeoffs already in the plan's own Production Decision Registry, explicitly skip-listed with reasons (`docs/execution-log/09-governance-security.md`) |
| 15 | Security | Unity Catalog masking hides PII from analyst role | Data Engineering | ✅ Verified and active — re-implemented as `silver.streaming_transactions_masked`, a view that computes the mask at query time instead of altering the raw table, so it doesn't touch the base table's schema at all. Non-privileged query via the SQL Warehouse returns `.xxx.xxx`/`dev_****`; the raw table read from the DBR 14.3 interactive cluster (the exact reproduction of the original break) still returns `31265` rows with no error (`docs/execution-log/10-followup-fixes.md`) |
| 16 | Chaos | All 5 resilience scenarios pass | Platform Team | ✅ 5/5 pass — `test_03` (sequential p99 < 100ms) was previously blocked by `_get_thresholds()` blocking the request path on a synchronous App Config call whenever its 60s cache TTL expired; fixed with a background stale-while-revalidate refresh instead. p99 dropped from 178.97ms to 1.38ms, redeployed and re-verified live (`docs/execution-log/10-followup-fixes.md`) |
| 17 | Documentation | Model Card, MLOps Runbook, and Security Architecture complete | All Teams | ✅ Present — `docs/model_card_hybrid_v1.md`, `docs/mlops_runbook.md`, `docs/security_architecture.md`, plus the full `docs/execution-log/` series covering every phase's actual execution |

## Known, deliberately-undone items (not blocking, tracked)

- **PCI-DSS-adjacent dependency CVE** (`cryptography` PYSEC-2026-3552): affects only S/MIME `EnvelopedData` auto-decryption, which nothing in this codebase implements; blocked from a version bump by mlflow's own upstream `cryptography<50` pin. Re-audit when mlflow relaxes it.
- **Entra ID → Unity Catalog account-level group sync**: 5 real Entra ID groups and 5 Databricks workspace-level groups were created, but Unity Catalog `GRANT` statements resolve principals against Databricks *account*-level identity, which requires Account Console-level SCIM configuration not achievable headlessly this session. The masking mechanism itself (which doesn't depend on group GRANTs) is proven working.
- **Private Endpoints**: still disabled (`enable_private_endpoints = false`), as designed for Free Trial.
- **Logic Apps** (Phase 5): now deployed and verified end-to-end (`docs/execution-log/10-followup-fixes.md`) — no longer a gap.

## Sign-Off

| Role | Name | Date | Signature |
|---|---|---|---|
| Data Engineering Lead | | | |
| Data Science Lead | | | |
| Platform Engineering Lead | | | |
| Security/Compliance | | | |
