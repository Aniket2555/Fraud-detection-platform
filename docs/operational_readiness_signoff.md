# Operational Readiness Sign-Off Checklist

## Pre-Production Verification

| # | Category | Check | Owner | Status |
|---|---|---|---|---|
| 1 | Infrastructure | All Bicep modules deploy successfully | Platform Team | ☐ |
| 2 | Infrastructure | Key Vault secrets populated and rotated | Platform Team | ☐ |
| 3 | Data Pipeline | Bronze → Silver → Gold Medallion pipeline processes IEEE-CIS dataset | Data Engineering | ☐ |
| 4 | Data Pipeline | Streaming pipeline ingests Event Hubs events and writes to Silver Delta | Data Engineering | ☐ |
| 5 | Feature Store | 43 features compute correctly with PIT join (zero temporal leakage) | Data Engineering | ☐ |
| 6 | ML Model | Hybrid ensemble achieves PR-AUC ≥ 0.80 on test set | Data Science | ☐ |
| 7 | ML Model | Scoring endpoint responds < 100ms (p99) | Data Science | ☐ |
| 8 | Decision Engine | 4 score bands route correctly (approve/step_up/manual_review/block) | Platform Team | ☐ |
| 9 | Decision Engine | Service Bus fan-out delivers to all 3 subscriptions | Platform Team | ☐ |
| 10 | Case Management | Azure SQL schema deployed (5 tables + 2 stored procedures) | Platform Team | ☐ |
| 11 | MLOps | Daily drift check runs and logs to Gold history table | Data Science | ☐ |
| 12 | MLOps | Champion-Challenger gate evaluates all 4 metrics | Data Science | ☐ |
| 13 | Security | TruffleHog scan passes with zero verified leaks | Platform Team | ☐ |
| 14 | Security | Checkov IaC scan passes with zero critical violations | Platform Team | ☐ |
| 15 | Security | Unity Catalog masking hides PII from analyst role | Data Engineering | ☐ |
| 16 | Chaos | All 5 resilience scenarios pass | Platform Team | ☐ |
| 17 | Documentation | Model Card, MLOps Runbook, and Security Architecture complete | All Teams | ☐ |

## Sign-Off

| Role | Name | Date | Signature |
|---|---|---|---|
| Data Engineering Lead | | | |
| Data Science Lead | | | |
| Platform Engineering Lead | | | |
| Security/Compliance | | | |
