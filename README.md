# Real-Time Fraud Detection Platform

A production-grade, end-to-end real-time credit card fraud detection platform built on Azure, Databricks, Event Hubs, Delta Lake, PyTorch/XGBoost hybrid ensemble scoring, Azure Functions fast-path decision engine, Logic Apps analyst case management, multi-metric MLOps drift monitoring, and Unity Catalog zero-trust security.

---

## Architecture Overview

```
[ Transactions Source / Kaggle Replay ]
                 │
   ┌─────────────┴─────────────┐
   ▼                           ▼
[ ADF Batch Landing ]   [ Event Hubs Streaming ] (4 Partitions, Card-ID Key, Avro Capture)
   │                           │
   ▼                           ▼
[ Bronze Delta ] ──────► [ Silver Delta Stream ] (Idempotent MERGE + Sentinel Nulls + PII Hashing)
                               │
                               ▼
               [ Feature Store (Delta Gold / Cosmos DB Gremlin) ]
               (43 Features: Stateless, Velocity, Geo, Baselines, Merchant, Graph)
                               │
                               ▼
               [ ML Hybrid Ensemble (MLflow PyFunc) ]
               (XGBoost + PyTorch Autoencoder + Isolation Forest + Stacking Meta-Learner)
                               │
                               ▼
[ Azure SQL Case DB ] ◄─── [ Service Bus Topic ] ◄─── [ Fast-Path Decision Engine ]
(5 Tables + MERGE)           (3 Subscriptions)          (<15ms Azure Function + App Config)
         │                                                        │
         ▼                                                        ▼
[ Logic Apps Workflow ]                                 [ Compliance Audit Logger ]
(5-min OTP Wait + 30-min Analyst Escalation)            (Immutable Audit Delta Table)
                                                                  │
                                                                  ▼
                                                        [ Daily MLOps Drift & Retrain Job ]
                                                        (PSI/KS/JSD + 4-Gate Champion/Challenger)
```

---

## Phase Implementation Status (100% Complete 🎉)

- [x] **Phase 0:** IaC & Environment Foundation (Bicep IaC, Key Vault, ADLS Gen2, Databricks Workspace, Unity Catalog, CI/CD, Budget Controls)
- [x] **Phase 1:** Batch Ingestion & Baseline Model (ADF Ingestion, Auto Loader, PyDeequ Quality Gates, Medallion Bronze/Silver/Gold, XGBoost Baseline)
- [x] **Phase 2:** Real-Time Streaming Ingestion (Event Hubs Standard, Avro Capture, Python Stream Producer, Structured Streaming Bronze/Silver MERGE)
- [x] **Phase 3:** Feature Engineering & Feature Store (43 Features, 6 Families, Haversine >900 km/h Impossible Travel, Cosmos DB Gremlin + GraphFrames, PIT Join Engine)
- [x] **Phase 4:** Hybrid Model & Real-Time Serving (XGBoost + PyTorch Deep Autoencoder + Isolation Forest, Isotonic Calibrators, Stacking Meta-Learner, MLflow PyFunc, SHAP Explainer, <100ms `score.py`, 3-Tier Circuit Breaker)
- [x] **Phase 5:** Decision Engine & Case Workflow (Azure App Config 60s TTL Cache, Service Bus 3 Subscriptions, Azure SQL Serverless 5 Tables, Fast-Path Function <15ms, Compliance Audit Logger, Timer DLQ Monitor, Logic Apps Step-Up)
- [x] **Phase 6:** MLOps Loop (Continuous Retraining, PSI/KS/JSD Multi-Metric Drift, Concept Drift, 4-Gate Champion-Challenger + McNemar's Test, Shadow Scoring, Rollback Sentinel)
- [x] **Phase 7:** Governance, Security & Hardening (Unity Catalog Column Masking, SHA-256 PII Salting, Private Endpoints Bicep, 4-Scan Security CI/CD, 5-Scenario Chaos Suite, 17-Item Sign-Off)

---

## Repository Structure

```
.
├── infrastructure/               # Bicep IaC modules & deployment scripts
│   ├── main.bicep                # Sub-scope deployment orchestrator
│   └── modules/                  # RG, Log Analytics, Key Vault, Storage, Databricks, Event Hubs, Cosmos DB, Service Bus, Azure SQL, App Config, RBAC, Private Endpoints, Diagnostic Settings
├── data-factory/                 # ADF pipelines, datasets, linked services, & triggers
├── database/                     # Azure SQL migrations (V001-V005) & stored procedures (sp_upsert_fraud_case, sp_update_case_status)
├── databricks/                   # PySpark Medallion notebooks, quality gates, feature modules, streaming MERGE, GraphFrames, governance, & MLOps jobs
│   ├── governance/               # Unity Catalog column-level PII masking policies & RBAC grants DDL
│   ├── src/                      # Python source packages (quality, transformations, schemas, features, mlops, security)
│   ├── notebooks/                # Bronze, Silver, Gold, features, validation, monitoring, and MLOps notebooks
│   └── jobs/                     # Databricks Workflow JSON job definitions (Phase 1, 2, 3, 6)
├── producers/                    # Async Python stream producer (Kaggle Credit Card dataset replay with deterministic synthesis)
├── schemas/                      # Canonical TransactionEvent JSON Schema v1.0 specification
├── ml/                           # Hybrid ML ensemble, Autoencoder, XGBoost, Optuna, calibrators, meta-learner, PyFunc, scoring script, SHAP explainer, circuit breaker
│   ├── training/                 # Data preparation loader, supervised XGBoost, PyTorch Autoencoder, Isolation Forest, calibrators, meta-learner
│   ├── data_augmentation/        # SMOTE synthetic fraud generator with 3:1 ratio cap
│   ├── ensemble/                 # Custom MLflow PyFunc wrapper (FraudEnsemblePyFunc)
│   ├── pipelines/                # 9-step training pipeline orchestration + PR-AUC >= 0.80 quality gate
│   ├── serving/                  # Endpoint score.py, SHAP TreeExplainer, thread-safe circuit breaker, deployment_spec.yaml
│   └── tests/                    # PyTest unit tests & latency SLA benchmark
├── functions/                    # Fast-Path Decision Engine, Compliance Audit Logger, & Timer-Triggered DLQ Monitor Azure Functions
├── logic-apps/                   # Step-Up authentication & case management Logic Apps workflow definitions
├── tests/                        # Chaos engineering suite (5 resilience failure scenarios)
├── scripts/                      # Secret rotation, DLQ replay handlers, smoke tests, and 8-step platform verification
├── docs/                         # Phase guides, feature store catalog, model card, case management runbooks, MLOps runbooks, security architecture, and DR guides
└── Implementation-details/       # Exhaustive phase-by-phase technical plans (Phases 0–7)
```

---

## Getting Started

### Quick Start Infrastructure Deployment
1. Update parameters in `infrastructure/modules/parameters/dev.parameters.json`.
2. Run Bicep Deployment:
   ```bash
   az deployment sub create \
     --location centralindia \
     --template-file infrastructure/main.bicep \
     --parameters infrastructure/modules/parameters/dev.parameters.json
   ```
3. Run Platform Verification Script:
   ```bash
   bash scripts/verify_platform_end_to_end.sh
   ```

### Full Documentation Suite
- [Free Trial Budget Guide](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/phase0/free_trial_budget_guide.md)
- [Production Upgrade Guide](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/phase0/upgrade_to_production.md)
- [Dataset Analysis & Schema](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/phase1/dataset_analysis.md)
- [Baseline Model Report](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/phase1/baseline_model_report.md)
- [Streaming Recovery Runbook](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/runbooks/streaming_recovery.md)
- [Feature Store Catalog (43 Features)](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/feature_store_catalog.md)
- [Hybrid Model Card v1](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/model_card_hybrid_v1.md)
- [Case Management Workflow & API Contract](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/case_management_workflow.md)
- [MLOps Operational Runbook](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/mlops_runbook.md)
- [Security Architecture & PCI-DSS Scope](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/security_architecture.md)
- [Operational Readiness Sign-Off Checklist](file:///d:/code%20file/Project-2-Real-time-fraudlent%20detection/docs/operational_readiness_signoff.md)
