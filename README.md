<div align="center">

# 🛡️ Real-Time Fraud Detection Platform

**A production-grade, end-to-end credit card fraud detection system built on Azure**

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-MLflow-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-844FBA?logo=terraform&logoColor=white)](https://terraform.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)](.github/workflows)

> Detects fraudulent credit card transactions **in real time** with a hybrid ML ensemble (XGBoost + PyTorch Autoencoder + Isolation Forest), a fast-path decision engine under **15 ms**, and a fully automated MLOps retraining loop — governed by Unity Catalog zero-trust security and PCI-DSS-aligned controls.

</div>

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Prerequisites](#-prerequisites)
- [Getting Started](#-getting-started)
- [Local Development](#-local-development)
- [Running Tests](#-running-tests)
- [CI/CD Pipelines](#-cicd-pipelines)
- [Documentation](#-documentation)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                             │
│                                                                     │
│   [ Transactions Source / Kaggle Replay ]                           │
│          │                                                          │
│   ┌──────┴──────────────┐                                           │
│   ▼                     ▼                                           │
│ [ ADF Batch Landing ]  [ Event Hubs Streaming ]                     │
│                         (4 Partitions · Card-ID Key · Avro Capture) │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    MEDALLION LAKEHOUSE                              │
│                                                                     │
│   [ Bronze Delta ] ──► [ Silver Delta Stream ]                      │
│                        (Idempotent MERGE · Sentinel Nulls · PII Hash)│
│                                 │                                   │
│                                 ▼                                   │
│          [ Feature Store (Delta Gold / Cosmos DB Gremlin) ]         │
│          (43 Features: Stateless · Velocity · Geo ·                 │
│           Baselines · Merchant · Graph)                             │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    HYBRID ML ENSEMBLE                               │
│                                                                     │
│   XGBoost + PyTorch Autoencoder + Isolation Forest                  │
│           │       (Isotonic Calibrators)                            │
│           └──► Stacking Meta-Learner (MLflow PyFunc)                │
│                        │  SHAP Explainability                       │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    DECISION & WORKFLOW LAYER                        │
│                                                                     │
│   [ Fast-Path Decision Engine ]                                     │
│     (Azure Function · <15 ms · App Config 60s TTL Cache)           │
│          │                                                          │
│          ▼                                                          │
│   [ Service Bus Topic ] ──► [ Azure SQL Case DB ]                  │
│    (3 Subscriptions)         (5 Tables · MERGE Upsert)              │
│          │                          │                               │
│          ▼                          ▼                               │
│   [ Compliance Audit ]    [ Logic Apps Workflow ]                   │
│    (Immutable Delta)       (5-min OTP · 30-min Escalation)         │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    MLOPS & GOVERNANCE LAYER                         │
│                                                                     │
│   [ Daily Drift & Retrain Job ]                                     │
│   (PSI · KS · JSD · 4-Gate Champion/Challenger · McNemar's Test)   │
│                                                                     │
│   [ Unity Catalog ] · [ Private Endpoints ] · [ Security CI/CD ]   │
│   (Column Masking · SHA-256 PII Salting · 5-Scenario Chaos Suite)  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

| Feature | Details |
|---|---|
| ⚡ **Ultra-low Latency** | Fast-path scoring decision under **15 ms** via Azure Functions |
| 🤖 **Hybrid ML Ensemble** | XGBoost + PyTorch Deep Autoencoder + Isolation Forest + Stacking Meta-Learner |
| 📊 **43-Feature Store** | Stateless, velocity, geo, baseline, merchant, and graph features |
| 🌊 **Real-Time Streaming** | Azure Event Hubs → Structured Streaming → Delta Lake Medallion |
| 🔄 **Automated MLOps** | PSI/KS/JSD drift detection → auto-retrain → 4-gate champion/challenger |
| 🔐 **Zero-Trust Security** | Unity Catalog column masking, SHA-256 PII salting, private endpoints |
| 🧪 **Chaos Engineering** | 5-scenario resilience test suite for production hardening |
| 📋 **Compliance Ready** | Immutable audit log, PCI-DSS scope, 17-item operational sign-off |
| 🗺️ **Impossible Travel** | Haversine >900 km/h velocity geo-check for account takeover signals |
| 🕸️ **Graph Features** | Cosmos DB Gremlin + GraphFrames for merchant/card relationship signals |

---

## 🛠️ Tech Stack

<details>
<summary><b>Cloud & Infrastructure</b></summary>

| Component | Service |
|---|---|
| Cloud Platform | Microsoft Azure |
| Infrastructure as Code | Terraform (modular) |
| Data Lakehouse | Azure Data Lake Storage Gen2 + Delta Lake |
| Streaming | Azure Event Hubs (4 partitions, Avro Capture) |
| Batch Ingestion | Azure Data Factory |
| Secrets & Config | Azure Key Vault + Azure App Configuration |
| Messaging | Azure Service Bus (3 subscriptions) |
| Graph Database | Azure Cosmos DB (Gremlin API) |
| Relational DB | Azure SQL Serverless |
| Serverless Compute | Azure Functions |
| Workflow Automation | Azure Logic Apps |
| Monitoring | Azure Monitor + Budget Alerts |

</details>

<details>
<summary><b>Data & ML</b></summary>

| Component | Library / Service |
|---|---|
| Processing Engine | Apache Spark (Databricks) |
| Data Quality | PyDeequ |
| Feature Store | Delta Gold Tables + Cosmos DB Gremlin |
| Experiment Tracking | MLflow |
| ML Governance | Databricks Unity Catalog |
| Gradient Boosting | XGBoost ≥ 2.0 |
| Deep Learning | PyTorch ≥ 2.2 (Deep Autoencoder) |
| Anomaly Detection | Scikit-learn Isolation Forest |
| HPO | Optuna |
| Imbalanced Data | imbalanced-learn (SMOTE) |
| Explainability | SHAP |
| Calibration | Isotonic Regression (Scikit-learn) |

</details>

<details>
<summary><b>DevOps & Quality</b></summary>

| Component | Tool |
|---|---|
| CI/CD | GitHub Actions (4 pipelines) |
| Testing | Pytest + pytest-cov + pytest-asyncio |
| Linting | Black + isort + Flake8 + mypy |
| Security Scanning | Bandit + pip-audit |
| Code Ownership | GitHub CODEOWNERS |

</details>

---

## 📁 Repository Structure

```
fraud-detection-platform/
│
├── .github/                          # GitHub configuration
│   ├── workflows/                    # CI/CD pipelines (infra, ML, data, security)
│   ├── CODEOWNERS                    # Code ownership rules
│   └── pull_request_template.md      # PR template
│
├── infrastructure/                   # Terraform IaC (modular)
│   ├── main.tf                       # Root module — wires all sub-modules
│   ├── modules/                      # RG, Key Vault, Storage, Databricks, Event Hubs,
│   │                                 # Cosmos DB, Service Bus, SQL, App Config,
│   │                                 # RBAC, Private Endpoints, Diagnostic Settings
│   ├── environments/                 # Per-environment .tfvars (dev.tfvars)
│   └── bootstrap/                    # One-time remote state storage bootstrap
│
├── data-factory/                     # ADF pipelines, datasets, linked services, triggers
│
├── database/                         # Azure SQL migrations (V001–V005)
│                                     # Stored procs: sp_upsert_fraud_case,
│                                     # sp_update_case_status
│
├── databricks/                       # PySpark Medallion notebooks & source packages
│   ├── notebooks/                    # Bronze, Silver, Gold, features, validation,
│   │                                 # monitoring, and MLOps notebooks
│   ├── src/                          # Python packages: quality, transformations,
│   │                                 # schemas, features, mlops, security
│   ├── governance/                   # Unity Catalog column-level PII masking & RBAC DDL
│   └── jobs/                         # Databricks Workflow JSON job definitions
│
├── producers/                        # Async Python stream producer
│                                     # (Kaggle Credit Card dataset replay)
│
├── schemas/                          # Canonical TransactionEvent JSON Schema v1.0
│
├── ml/                               # Hybrid ML ensemble pipeline
│   ├── training/                     # Data loader, XGBoost, PyTorch Autoencoder,
│   │                                 # Isolation Forest, calibrators, meta-learner
│   ├── data_augmentation/            # SMOTE synthetic fraud generator (3:1 ratio cap)
│   ├── ensemble/                     # MLflow PyFunc wrapper (FraudEnsemblePyFunc)
│   ├── pipelines/                    # 9-step training pipeline, PR-AUC ≥ 0.80 gate
│   ├── serving/                      # score.py, SHAP TreeExplainer, circuit breaker,
│   │                                 # deployment_spec.yaml
│   └── tests/                        # PyTest unit tests & latency SLA benchmark
│
├── functions/                        # Azure Functions
│   ├── decision_engine/              # Fast-Path Decision Engine (<15 ms)
│   ├── audit_logger/                 # Compliance Audit Logger (immutable Delta writes)
│   └── dlq_monitor/                  # Timer-triggered Dead-Letter Queue monitor
│
├── logic-apps/                       # Logic Apps workflow definitions
│                                     # (Step-Up OTP + Analyst escalation)
│
├── tests/                            # Chaos engineering suite (5 resilience scenarios)
│
├── scripts/                          # Operational scripts
│                                     # (secret rotation, DLQ replay, smoke tests,
│                                     # 8-step platform verification)
│
├── docs/                             # Full documentation suite (see Documentation)
│
├── pyproject.toml                    # Package config (fraud_detection namespace)
├── requirements.txt                  # Root dev/CI dependencies
├── requirements-dev.txt              # Additional dev tooling
└── .gitignore
```

---

## 📋 Prerequisites

Before you begin, ensure you have the following:

**Tools:**
- [Python 3.10+](https://python.org/downloads/)
- [Terraform ≥ 1.6](https://developer.hashicorp.com/terraform/install)
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- [Git](https://git-scm.com/)

**Azure Resources** (all provisioned automatically by Terraform):
- Active Azure Subscription with Contributor access
- Databricks workspace
- Azure Event Hubs namespace
- Azure Key Vault
- Azure Data Lake Storage Gen2
- Azure SQL Serverless
- Azure Cosmos DB (Gremlin API)
- Azure Service Bus
- Azure App Configuration

> **💡 Tip:** Use an Azure Free Trial for dev/test. See [`docs/phase0/free_trial_budget_guide.md`](docs/phase0/free_trial_budget_guide.md) for cost controls and budget alerts.

---

## 🚀 Getting Started

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Aniket2555/Fraud-detection-platform.git
cd Fraud-detection-platform
```

### Step 2 — Bootstrap Remote State *(One-Time Only)*

Terraform cannot manage the storage account that stores its own state. Run this first:

```bash
cd infrastructure/bootstrap
terraform init
terraform apply -var="environment=dev"
# Copy the printed backend_config_snippet into ../backend-dev.conf
# (See infrastructure/backend-dev.conf.example for the format)
```

### Step 3 — Configure Environment Variables

```bash
# Fill in your Azure-specific values
cp infrastructure/environments/dev.tfvars.example infrastructure/environments/dev.tfvars
# Edit: owner_email, deployer_object_id
```

### Step 4 — Deploy Infrastructure

```bash
cd infrastructure
terraform init -backend-config=backend-dev.conf
export TF_VAR_sql_admin_password="<your-strong-password>"   # ⚠️ Never commit this!
terraform plan  -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

### Step 5 — Verify Platform

```bash
bash scripts/verify_platform_end_to_end.sh
```

This 8-step script validates connectivity to all Azure services and confirms the end-to-end pipeline is operational.

---

## 💻 Local Development

### Install Python Dependencies

```bash
# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS

# Install the fraud_detection namespace package in editable mode
pip install -e .
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

> **Note:** `pyproject.toml` maps the `fraud_detection` Python namespace across both `databricks/src/` and `ml/`. Running `pip install -e .` is required for imports to resolve correctly in both local dev and CI.

### Start the Stream Producer (Local Replay)

The transaction producer replays the Kaggle Credit Card Fraud dataset into Event Hubs:

```bash
cd producers/transaction_producer
# Configure your Event Hubs connection string via .env or environment variable
python producer.py
```

---

## 🧪 Running Tests

```bash
# All unit tests with coverage report
pytest --cov=fraud_detection --cov-report=term-missing

# ML model tests only
pytest ml/tests/

# Latency SLA benchmark (requires a live scoring endpoint)
pytest ml/tests/test_latency_sla.py -v

# Chaos engineering scenarios (requires full Azure infrastructure)
pytest tests/ -v
```

**Code Quality & Security:**

```bash
black --check .               # Style formatting check
isort --check-only .          # Import order check
flake8 .                      # Linting
mypy databricks/src ml/       # Static type checking
bandit -r databricks/src ml/  # Security scan
pip-audit                     # Dependency vulnerability audit
```

---

## ⚙️ CI/CD Pipelines

Four GitHub Actions workflows run automatically on push and pull requests:

| Workflow | File | Trigger | What It Does |
|---|---|---|---|
| **Infrastructure Deploy** | [infra-deploy.yml](.github/workflows/infra-deploy.yml) | Push to `main` | Terraform plan + apply for all Azure resources |
| **ML CI** | [ml-ci.yml](.github/workflows/ml-ci.yml) | Push / PR | Lint, type-check, unit tests, PR-AUC quality gate |
| **Data CI** | [data-ci.yml](.github/workflows/data-ci.yml) | Push / PR | Schema validation, PyDeequ data quality checks |
| **Security Scan** | [security-scan.yml](.github/workflows/security-scan.yml) | Push / PR | Bandit + pip-audit + 4-scan security suite |

---

## 📚 Documentation

| Document | Description |
|---|---|
| [End-to-End Guide](docs/end_to_end_guide.md) | Complete walkthrough of the full platform |
| [Free Trial Budget Guide](docs/phase0/free_trial_budget_guide.md) | Azure cost controls and budget alerts for dev/test |
| [Production Upgrade Guide](docs/phase0/upgrade_to_production.md) | Steps to promote from dev to production |
| [Dataset Analysis & Schema](docs/phase1/dataset_analysis.md) | Kaggle dataset analysis and `TransactionEvent` schema |
| [Baseline Model Report](docs/phase1/baseline_model_report.md) | XGBoost baseline metrics and evaluation results |
| [Streaming Recovery Runbook](docs/runbooks/streaming_recovery.md) | Ops runbook for stream failure recovery |
| [Feature Store Catalog (43 Features)](docs/feature_store_catalog.md) | Full feature definitions, families, and SQL |
| [Hybrid Model Card v1](docs/model_card_hybrid_v1.md) | Model metadata, performance, fairness, and limitations |
| [Case Management Workflow & API](docs/case_management_workflow.md) | Service Bus topics, Logic Apps flow, SQL schema |
| [MLOps Operational Runbook](docs/mlops_runbook.md) | Drift thresholds, retraining gates, rollback steps |
| [Security Architecture & PCI-DSS Scope](docs/security_architecture.md) | Zero-trust controls, private endpoints, PCI scope |
| [Operational Readiness Sign-Off](docs/operational_readiness_signoff.md) | 17-item production readiness checklist |

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes following the code style (Black + isort + Flake8)
4. Run the full test suite: `pytest --cov=fraud_detection`
5. Push your branch and open a Pull Request using the [PR template](.github/pull_request_template.md)

> Please review [CODEOWNERS](.github/CODEOWNERS) to understand required reviewers for each area of the codebase.

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

Copyright © 2026 Aniket Tiwari

---

<div align="center">

**Built with ❤️ on Azure · Databricks · Delta Lake · MLflow**

⭐ If you find this project useful, please consider giving it a star!

</div>
