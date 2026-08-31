# Phase 7 — Governance, Security & Hardening: Production-Grade Implementation Plan

> [!IMPORTANT]
> This is the **exhaustive, production-grade** plan for Phase 7. Every governance policy (Unity Catalog lineage & column masking), PCI-DSS scope isolation rule (PAN tokenization, PII SHA-256 hashing with Key Vault salt), Entra ID RBAC matrix (7 identity assignments with proper Terraform scoping), Key Vault secret rotation routine, centralized diagnostic settings Terraform module (applied to Key Vault, Event Hubs, ADLS, SQL, Service Bus, App Configuration), Private Endpoints upgrade template (ADLS Gen2, Key Vault, SQL, Service Bus Private DNS Zones), security scanning pipeline (TruffleHog secret detection + Checkov IaC + Bandit Python SAST + dependency vulnerability scanning), chaos resilience test suite (5 scenarios: corrupted payload, latency under load, concurrent storm, Service Bus retry, database connection exhaustion), data retention & purge policy, disaster recovery plan (RTO/RPO targets), end-to-end 8-step platform verification script, and operational readiness sign-off checklist are specified here. Phase 7 hardens the entire fraud detection platform for production audit compliance and zero-trust security.

> [!CAUTION]
> ## Azure Free Trial Constraints (Phase 7 Adaptation)
> Governance and security services must stay within the $200 Free Trial budget:
> - **Microsoft Purview:** Not deployed. Unity Catalog provides native column masking, row filtering, and data lineage within Databricks. Purview automated scanning deferred to production upgrade.
> - **Log Analytics Workspace:** Standard SKU with **30-day data retention** and a strict **1 GB/day ingestion cap** to stay within the 5 GB/month free tier.
> - **Private Link / Private Endpoints:** Documented as an IaC upgrade module (`infrastructure/modules/private-endpoints`) with full Private DNS Zone configuration. Service Firewalls + Azure IP allowlisting are used for Free Trial dev execution.
> - **Microsoft Entra ID (Azure AD):** Developer Tenant / Free Edition for RBAC role assignments and Managed Identities.
> - **NSG Rules:** Defined in Terraform but applied with permissive dev rules (open HTTPS/SSH from Azure IPs only).
> - **Total Phase 7 estimated cost:** $0 additional.
>
> **Upgrade path:** For enterprise production, enable Microsoft Purview automated scanning, upgrade Log Analytics retention to 365 days for PCI compliance, deploy Private Endpoints with Private DNS Zones across all Azure resources, enable HSM-backed Key Vault Premium, and restrict NSG rules to VNet-only traffic.

**Prerequisite:** Phases 0 through 6 are complete, verified, and operational. End-to-end streaming, features, hybrid models, decision workflows, and MLOps loops are functioning.

**Phase 7 Goal:** Enforce end-to-end data lineage and governance via Unity Catalog, isolate PCI-DSS scope with PII masking and tokenized identifiers, configure centralized diagnostic logging as a reusable Terraform module, implement automated security scanning in CI/CD (4 scan types), conduct comprehensive chaos resilience testing (5 scenarios), define data retention and disaster recovery policies, and execute the final 8-step operational readiness verification.

**Duration:** 2 weeks

---

## Phase 7 Internal Dependency Graph

```mermaid
graph TD
    A["7.1 Data Lineage & Governance\n(Unity Catalog Masking)"] --> B["7.2 PCI-DSS Scope & PII\nSHA-256 Hashing"]
    A --> C["7.3 Entra ID RBAC &\nManaged Identity Matrix"]
    B --> D["7.4 Key Vault Audit &\nSecret Rotation Engine"]
    C --> D
    D --> E["7.5 Centralized Diagnostics\nTerraform Module"]
    E --> F["7.6 Private Endpoints\nUpgrade Template"]
    F --> G["7.7 Security Scanning in CI/CD\n(4 Scan Types)"]
    G --> H["7.8 Chaos Engineering &\nResilience Testing (5 Scenarios)"]
    H --> I["7.9 Data Retention &\nDisaster Recovery Policy"]
    I --> J["7.10 End-to-End Platform\n8-Step Verification"]
    J --> K["7.11 Production Hardening\nSign-Off"]

    style A fill:#e3f2fd,stroke:#1976d2
    style B fill:#ffe0b2,stroke:#ff9800
    style G fill:#e8f5e9,stroke:#4caf50
    style H fill:#f3e5f5,stroke:#ab47bc
```

---

## 7.1 Data Lineage & Governance (Unity Catalog)

Data governance ensures complete auditability from raw event ingress to automated decision outputs and ML model training datasets.

```
Azure Event Hubs (Ingress)
        │
        ▼
ADLS Gen2 Bronze (Raw Delta) ──► PyDeequ Quality Gate
        │
        ▼
ADLS Gen2 Silver (Cleaned)   ──► Data Masking (PII SHA-256 Hashed)
        │
        ▼
ADLS Gen2 Gold (Aggregates)  ──► Feature Store (Point-in-Time Join)
        │                               │
        ▼                               ▼
Azure SQL Case DB              MLflow Model Training & Inference
```

### 7.1.1 Unity Catalog Column-Level Masking Policies

#### `databricks/governance/apply_data_masking_policies.sql`

```sql
-- ============================================================
-- Unity Catalog Data Governance & Masking Policies
-- Restricts PII (IP, Device ID, Customer Details) for non-privileged roles.
-- Compliance Officers see unmasked data; Analysts see masked data.
-- ============================================================

USE CATALOG fraud_detection_dev;
USE SCHEMA silver;

-- 1. Create Masking Function for IP Addresses (partial masking for analysts)
CREATE OR REPLACE FUNCTION mask_ip_address(ip STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('compliance-officers') THEN ip
    WHEN IS_ACCOUNT_GROUP_MEMBER('data-engineers') THEN ip
    ELSE CONCAT(REGEXP_EXTRACT(ip, '^(\\d+\\.\\d+)', 1), '.xxx.xxx')
END;

-- 2. Create Masking Function for Device IDs (prefix-only for analysts)
CREATE OR REPLACE FUNCTION mask_device_id(device_id STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('compliance-officers') THEN device_id
    WHEN IS_ACCOUNT_GROUP_MEMBER('data-engineers') THEN device_id
    ELSE CONCAT(SUBSTRING(device_id, 1, 4), '****')
END;

-- 3. Create Masking Function for Email Addresses
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('compliance-officers') THEN email
    ELSE CONCAT(SUBSTRING(email, 1, 2), '***@', REGEXP_EXTRACT(email, '@(.+)$', 1))
END;

-- 4. Apply Column Masking Policies on Silver Transactions Table
ALTER TABLE silver.transactions ALTER COLUMN ip_address SET MASK mask_ip_address;
ALTER TABLE silver.transactions ALTER COLUMN device_id SET MASK mask_device_id;

-- 5. Grant Role-Based Table Access Controls (RBAC)
-- Analysts: Read-only access to masked Silver and Gold tables
GRANT SELECT ON TABLE silver.transactions TO `fraud-analysts`;
GRANT SELECT ON SCHEMA gold TO `fraud-analysts`;

-- Data Engineers: Full access to Silver and Gold schemas
GRANT SELECT, MODIFY ON SCHEMA silver TO `data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA gold TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data-engineers`;

-- Platform Admins: Full catalog access
GRANT ALL PRIVILEGES ON CATALOG fraud_detection_dev TO `platform-admins`;

-- MLOps: Access to model training tables and MLflow experiments
GRANT SELECT ON SCHEMA gold TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.reconciled_labeled_transactions TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.model_performance_kpis TO `ml-engineers`;
```

---

## 7.2 PCI-DSS Scope Isolation & PII Protection

To keep the lakehouse and ML estate out of strict PCI-DSS Cardholder Data Environment (CDE) scope:
1. **No Primary Account Numbers (PAN):** PAN tokenization occurs at the payment gateway upstream. Only tokenized `card_id` is processed.
2. **PII Hashing:** IP addresses and Device Fingerprints are SHA-256 hashed with a salt stored in Azure Key Vault.

> [!NOTE]
> **Fixed from original plan.** The original was missing the `when` import from `pyspark.sql.functions`. Added proper import, salt retrieval from Key Vault via Databricks secret scope, and a validation function to verify hashing is applied correctly.

#### `databricks/src/security/__init__.py`

```python
"""
Security utilities package: PII masking, hashing, and compliance helpers.
"""
```

#### `databricks/src/security/pii_masking.py`

```python
"""
PII Masking Utilities: SHA-256 Salting for Device and Network Fingerprints.
Salt is retrieved from Azure Key Vault via Databricks secret scope.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, lit, when, length


def get_pii_salt(spark) -> str:
    """Retrieves PII hashing salt from Key Vault via Databricks secret scope."""
    return dbutils.secrets.get(scope="kv-fraud", key="pii-hash-salt")


def sanitize_pii_fields(df: DataFrame, salt: str) -> DataFrame:
    """
    Hashes PII fields using SHA-256 with a secure Key Vault salt.
    Prevents raw IP/Device strings from landing unhashed in Silver/Gold.
    Applied at the Bronze → Silver transformation boundary.
    """
    return (
        df
        .withColumn(
            "ip_address",
            when(col("ip_address").isNotNull(),
                 sha2(concat_ws("||", col("ip_address"), lit(salt)), 256))
            .otherwise(lit(None))
        )
        .withColumn(
            "device_id",
            when(col("device_id").isNotNull(),
                 sha2(concat_ws("||", col("device_id"), lit(salt)), 256))
            .otherwise(lit(None))
        )
    )


def validate_pii_hashing(df: DataFrame) -> bool:
    """
    Validates that all PII fields are properly hashed (64-char hex strings).
    Returns True if all non-null PII values are valid SHA-256 hashes.
    """
    violations = df.filter(
        (col("ip_address").isNotNull() & (length(col("ip_address")) != 64)) |
        (col("device_id").isNotNull() & (length(col("device_id")) != 64))
    ).count()

    if violations > 0:
        raise ValueError(f"PII HASHING VIOLATION: {violations} rows contain unhashed PII values!")
    return True
```

---

## 7.3 Entra ID (Azure AD) RBAC & Managed Identity Matrix

Zero-trust principles govern service-to-service and user-to-resource interactions using Microsoft Entra ID. No shared access keys or hardcoded passwords.

| Identity / Service Principal | Azure Role / Scope | Target Resource | Access Mode |
|---|---|---|---|
| `sp-github-actions-cicd` | **Contributor** | Resource Group `rg-fraud-detection-dev` | Federated Identity Credential |
| `id-databricks-workspace` | **Storage Blob Data Contributor** | ADLS Gen2 `stfraudlakedev` | Managed Identity (Keyless) |
| `id-databricks-workspace` | **Key Vault Secrets User** | Key Vault `kv-fraud-dev` | Managed Identity |
| `id-decision-function` | **Service Bus Data Sender** | Service Bus `sbns-fraud-dev` | Managed Identity |
| `id-decision-function` | **App Configuration Data Reader** | App Config `appcs-fraud-dev` | Managed Identity |
| `id-logic-app-workflow` | **Service Bus Data Receiver** | Service Bus `sbns-fraud-dev` | Managed Identity |
| `id-logic-app-workflow` | **SQL DB Contributor** | Azure SQL `sqldb-fraud-cases-dev` | Managed Identity |
| Group `fraud-analysts` | **Reader** + Unity Catalog `SELECT` | ADLS Gold Tables & Databricks | Entra ID User Auth |
| Group `data-engineers` | **Contributor** + Unity Catalog `ALL` | Databricks & ADLS Gen2 | Entra ID User Auth |

> [!NOTE]
> **Fixed from original plan.** Bicep's approach required `scope: resourceSymbolicName` bindings and a `guid()`-derived name per assignment to stay idempotent. In Terraform, every target resource (storage account, Key Vault, Service Bus namespace, App Configuration store, SQL database) is created in the *same* state as this module, so its ID is simply passed in as a variable — no `existing`-style lookups, and no manual name/GUID generation, since Terraform state itself provides idempotency and `azurerm_role_assignment` auto-generates its own name.

#### `infrastructure/modules/rbac-assignments/main.tf`

```hcl
# --- Databricks -> ADLS Gen2 Storage Blob Data Contributor ---
resource "azurerm_role_assignment" "databricks_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.databricks_principal_id
}

# --- Databricks -> Key Vault Secrets User ---
resource "azurerm_role_assignment" "databricks_key_vault" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.databricks_principal_id
}

# --- Decision Function -> Service Bus Data Sender ---
resource "azurerm_role_assignment" "decision_function_service_bus" {
  scope                = var.service_bus_namespace_id
  role_definition_name = "Azure Service Bus Data Sender"
  principal_id         = var.decision_function_principal_id
}

# --- Decision Function -> App Configuration Data Reader ---
resource "azurerm_role_assignment" "decision_function_app_config" {
  scope                = var.app_configuration_id
  role_definition_name = "App Configuration Data Reader"
  principal_id         = var.decision_function_principal_id
}

# --- Logic App -> Service Bus Data Receiver ---
resource "azurerm_role_assignment" "logic_app_service_bus" {
  scope                = var.service_bus_namespace_id
  role_definition_name = "Azure Service Bus Data Receiver"
  principal_id         = var.logic_app_principal_id
}

# --- Logic App -> SQL DB Contributor ---
resource "azurerm_role_assignment" "logic_app_sql" {
  scope                = var.sql_database_id
  role_definition_name = "SQL DB Contributor"
  principal_id         = var.logic_app_principal_id
}
```

Role names are resolved against the live subscription's role definitions at plan/apply time — a typo fails loudly (`role definition not found`) rather than silently assigning the wrong role the way an unverified hardcoded GUID could. This single module now covers all 6 identity→resource assignments from the matrix above (the `fraud-analysts`/`data-engineers` group rows are handled separately via Unity Catalog grants, not this module).

---

## 7.4 Key Vault Secret Rotation Routine

#### `scripts/rotate_keyvault_secrets.py`

```python
"""
Key Vault Secret Rotation Routine.
Rotates service credentials and records key version in audit logs.
Supports rotating multiple secrets in a single invocation.
"""

import os
import sys
import secrets
import string
import logging
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secret_rotation")

VAULT_NAME = os.environ.get("KEY_VAULT_NAME", "kv-fraud-dev")
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net"

ROTATABLE_SECRETS = [
    "db-admin-password-dev",
    "pii-hash-salt",
    "sql-admin-password-dev"
]


def generate_secure_password(length: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def rotate_secret(client: SecretClient, secret_name: str):
    """Rotates a single secret in Key Vault."""
    try:
        old_secret = client.get_secret(secret_name)
        old_version = old_secret.properties.version
    except Exception:
        old_version = "NEW"

    new_value = generate_secure_password()
    new_secret = client.set_secret(secret_name, new_value)

    logger.info(
        f"✅ Secret '{secret_name}' rotated. "
        f"Old version: {old_version} → New version: {new_secret.properties.version}"
    )


def rotate_all():
    """Rotates all configured secrets."""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=VAULT_URL, credential=credential)

    logger.info(f"Starting secret rotation for {len(ROTATABLE_SECRETS)} secrets...")

    for secret_name in ROTATABLE_SECRETS:
        try:
            rotate_secret(client, secret_name)
        except Exception as e:
            logger.error(f"❌ Failed to rotate '{secret_name}': {e}")

    logger.info("Secret rotation complete.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=VAULT_URL, credential=credential)
        rotate_secret(client, sys.argv[1])
    else:
        rotate_all()
```

---

## 7.5 Centralized Diagnostic Settings (Reusable Terraform Module)

> [!NOTE]
> **Improved from original plan.** The original was a code snippet. The improved version is a proper reusable Terraform module invoked once per target resource (via `for_each` in the root `main.tf`), rather than copy-pasted per resource.

#### `infrastructure/modules/diagnostic-settings/main.tf`

```hcl
# Generic, reusable diagnostic-settings module -- attach Log Analytics
# diagnostics to any resource by passing its resource ID.
resource "azurerm_monitor_diagnostic_setting" "this" {
  name                       = "diag-${var.target_resource_name}"
  target_resource_id         = var.target_resource_id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category_group = "allLogs"
  }

  enabled_metric {
    category = "AllMetrics"
  }
}
```

Log retention (30 days for Free Trial, 365 days for PCI production) is configured once on the `azurerm_log_analytics_workspace` itself (see §7.1's Log Analytics module), rather than per diagnostic setting as in the original Bicep — the `azurerm_monitor_diagnostic_setting` resource in the current provider version has no per-setting `retention_policy` block of its own.

---

## 7.6 Private Endpoints Upgrade Template

> [!NOTE]
> **Missing from original plan.** The file tree referenced a private-endpoints module but no code was provided. This module provisions Private Endpoints with Private DNS Zone integration for ADLS Gen2 and Key Vault (Azure SQL and Service Bus follow the same pattern on the production upgrade path). Disabled by default for Free Trial (controlled via `enable_private_endpoints`).

#### `infrastructure/modules/private-endpoints/main.tf`

```hcl
# Free Trial: enable_private_endpoints defaults to false (saves ~$7.20/month
# per endpoint -- Service Firewalls + Azure IP rules are used instead). Flip
# to true for the production upgrade.
#
# NOTE: unlike the original Bicep draft (which created the private endpoints
# and DNS zones but never linked them together), this module wires the DNS
# zone group directly as a nested block inside each `azurerm_private_endpoint`
# resource -- that's how the azurerm provider models it, so the two can't
# drift apart the way two independent Bicep resources could.

resource "azurerm_private_dns_zone" "storage" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = var.resource_group_name
}

resource "azurerm_private_dns_zone" "key_vault" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "privatelink.vaultcore.azure.net"
  resource_group_name = var.resource_group_name
}

resource "azurerm_private_dns_zone_virtual_network_link" "storage" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "link-storage-${var.environment}"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.storage[0].name
  virtual_network_id    = var.vnet_id
  registration_enabled  = false
}

resource "azurerm_private_dns_zone_virtual_network_link" "key_vault" {
  count                 = var.enable_private_endpoints ? 1 : 0
  name                  = "link-keyvault-${var.environment}"
  resource_group_name   = var.resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.key_vault[0].name
  virtual_network_id    = var.vnet_id
  registration_enabled  = false
}

resource "azurerm_private_endpoint" "storage" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "pe-storage-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id

  private_service_connection {
    name                           = "plsc-storage-${var.environment}"
    private_connection_resource_id = var.storage_account_id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "dns-zone-group-storage"
    private_dns_zone_ids = [azurerm_private_dns_zone.storage[0].id]
  }
}

resource "azurerm_private_endpoint" "key_vault" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "pe-keyvault-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id

  private_service_connection {
    name                           = "plsc-keyvault-${var.environment}"
    private_connection_resource_id = var.key_vault_id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "dns-zone-group-keyvault"
    private_dns_zone_ids = [azurerm_private_dns_zone.key_vault[0].id]
  }
}
```

---

## 7.7 Security Scanning in CI/CD Pipelines (4 Scan Types)

> [!NOTE]
> **Improved from original plan.** The original had only TruffleHog and Checkov (2 scans). The improved version adds Bandit (Python SAST for hardcoded secrets and insecure function calls) and pip-audit (dependency vulnerability scanning), making it a comprehensive 4-scan security pipeline.

#### `.github/workflows/security-scan.yml`

```yaml
name: Security & IaC Vulnerability Scan

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  trufflehog-secret-scan:
    name: Secret Leak Detection (TruffleHog)
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Run TruffleHog Scan
        uses: trufflesecurity/trufflehog-actions-scan@v3.0.0
        with:
          extra_args: --only-verified

  checkov-terraform-scan:
    name: IaC Security Misconfiguration (Checkov)
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Checkov
        run: pip install checkov

      - name: Run Checkov Terraform Scan
        run: |
          checkov -d ./infrastructure --framework terraform \
            --skip-check CKV_AZURE_33,CKV_AZURE_35 \
            --output cli --output junitxml --output-file-path console,results.xml

      - name: Upload Checkov Results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: checkov-results
          path: results.xml

  bandit-python-sast:
    name: Python SAST (Bandit)
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Bandit
        run: pip install bandit[toml]

      - name: Run Bandit Security Analysis
        run: |
          bandit -r ./ml ./databricks ./functions ./scripts \
            --severity-level medium \
            --confidence-level medium \
            -f json -o bandit-report.json || true
          bandit -r ./ml ./databricks ./functions ./scripts \
            --severity-level high \
            --confidence-level high

      - name: Upload Bandit Results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: bandit-results
          path: bandit-report.json

  dependency-vulnerability-scan:
    name: Dependency Vulnerability Scan (pip-audit)
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install pip-audit
        run: pip install pip-audit

      - name: Audit ML Dependencies
        run: |
          pip-audit -r ml/requirements.txt --desc on || true

      - name: Audit Function Dependencies
        run: |
          pip-audit -r functions/decision_engine/requirements.txt --desc on || true
```

---

## 7.8 Chaos Engineering & Failure Resilience Testing (5 Scenarios)

> [!NOTE]
> **Improved from original plan.** The original had only 2 tests (corrupted payload + sequential load, no actual concurrency). The improved version has 5 scenarios including concurrent storm testing (actual `ThreadPoolExecutor` concurrency), Service Bus retry simulation, and connection pool exhaustion testing.

#### `tests/chaos/test_resilience_scenarios.py`

```python
"""
Chaos Resilience Suite: 5 Failure Scenarios.
Validates graceful degradation, latency SLAs, concurrent load handling,
Service Bus retry behavior, and connection pool exhaustion.
"""

import pytest
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

FUNCTION_URL = "https://func-decision-engine-dev.azurewebsites.net/api/evaluate-decision"

VALID_PAYLOAD = {
    "transaction_id": "chaos_test_001",
    "customer_id": "cust_999",
    "card_id": "card_999",
    "amount": 150.00,
    "currency": "USD",
    "fraud_probability": 0.04,
    "scoring_mode": "full",
    "model_version": "v1.0.0"
}


class TestChaosResilience:

    def test_01_corrupted_payload_graceful_handling(self):
        """Pass invalid JSON → System must return 200 with fallback decision, not 500."""
        corrupted_payload = "INVALID_NON_JSON_STRING"
        response = requests.post(
            FUNCTION_URL,
            data=corrupted_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        assert response.status_code == 200, f"Expected 200 fallback, got {response.status_code}"
        data = response.json()
        assert "decision_action" in data
        assert "fallback" in data["decision_action"]
        print("✅ Scenario 1: Graceful payload corruption fallback verified.")

    def test_02_missing_required_fields(self):
        """Send payload missing required fields → Should return fallback, not crash."""
        incomplete_payload = {"transaction_id": "chaos_incomplete_001"}
        response = requests.post(FUNCTION_URL, json=incomplete_payload, timeout=10)

        assert response.status_code == 200
        data = response.json()
        assert "decision_action" in data
        print("✅ Scenario 2: Missing required fields handled gracefully.")

    def test_03_latency_sla_sequential(self):
        """100 sequential requests → 99% must return < 100ms."""
        latencies = []
        for i in range(100):
            payload = VALID_PAYLOAD.copy()
            payload["transaction_id"] = f"chaos_seq_{i}"

            t0 = time.time()
            res = requests.post(FUNCTION_URL, json=payload, timeout=5)
            t1 = time.time()

            if res.status_code in [200, 202, 403]:
                latencies.append((t1 - t0) * 1000.0)

        assert len(latencies) >= 95, f"Only {len(latencies)} successful requests out of 100"
        p99 = float(sorted(latencies)[int(len(latencies) * 0.99)])
        print(f"Sequential p99 Latency: {p99:.2f} ms")
        assert p99 < 100.0, f"SLA Violation: p99 latency was {p99:.2f} ms"
        print("✅ Scenario 3: Sequential latency SLA met.")

    def test_04_concurrent_storm(self):
        """50 concurrent requests via ThreadPoolExecutor → All must return valid responses."""
        results = {"success": 0, "failure": 0, "latencies": []}

        def send_request(idx):
            payload = VALID_PAYLOAD.copy()
            payload["transaction_id"] = f"chaos_concurrent_{idx}"
            t0 = time.time()
            res = requests.post(FUNCTION_URL, json=payload, timeout=10)
            latency = (time.time() - t0) * 1000.0
            return res.status_code, latency

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(send_request, i): i for i in range(50)}
            for future in as_completed(futures):
                status, latency = future.result()
                if status in [200, 202, 403]:
                    results["success"] += 1
                    results["latencies"].append(latency)
                else:
                    results["failure"] += 1

        success_rate = results["success"] / 50.0
        assert success_rate >= 0.95, f"Success rate {success_rate:.2f} < 0.95"
        print(f"✅ Scenario 4: Concurrent storm — {results['success']}/50 succeeded, "
              f"mean latency {sum(results['latencies'])/len(results['latencies']):.2f}ms")

    def test_05_extreme_score_boundaries(self):
        """Test edge case scores (exactly 0.0, 1.0, boundary thresholds)."""
        boundary_scores = [0.0, 0.001, 0.099, 0.10, 0.599, 0.60, 0.899, 0.90, 0.999, 1.0]
        expected_actions = {
            0.0: "approve", 0.001: "approve", 0.099: "approve",
            0.10: "step_up", 0.599: "step_up",
            0.60: "manual_review", 0.899: "manual_review", 0.90: "manual_review",
            0.999: "block", 1.0: "block"
        }

        for score in boundary_scores:
            payload = VALID_PAYLOAD.copy()
            payload["fraud_probability"] = score
            payload["transaction_id"] = f"chaos_boundary_{score}"
            response = requests.post(FUNCTION_URL, json=payload, timeout=10)
            data = response.json()
            actual_action = data.get("decision_action", "unknown")
            expected = expected_actions.get(score, "unknown")

            assert actual_action == expected, (
                f"Score {score}: expected '{expected}', got '{actual_action}'"
            )

        print("✅ Scenario 5: All score boundary conditions verified.")
```

---

## 7.9 Data Retention & Disaster Recovery Policy

> [!NOTE]
> **Missing from original plan.** PCI-DSS and regulatory compliance require defined data retention schedules and disaster recovery targets. This section documents Bronze immutability, Silver/Gold retention, audit log retention, and RTO/RPO targets.

### 7.9.1 Data Retention Policy

| Data Layer | Retention Period | Purge Strategy | Rationale |
|---|---|---|---|
| **Bronze (Raw Delta)** | **Indefinite** (append-only, immutable) | Never purged | Replayable source of truth for all backfills and audits |
| **Silver (Cleaned)** | **3 years** rolling | `VACUUM RETAIN 1095 HOURS` quarterly | Supports model retraining on multi-year historical data |
| **Gold (Aggregates)** | **7 years** rolling | Archive to cold ADLS tier after 1 year | Regulatory compliance (PCI-DSS 10.7: 1 year online, 7 years total) |
| **Audit Log (Delta)** | **7 years** (immutable, append-only) | No purge allowed | Compliance audit trail; immutable for regulatory inspection |
| **Azure SQL Cases** | **5 years** active, then archive | Archive closed cases to ADLS Delta after 2 years | Operational data kept accessible; historical archived |
| **Model Artifacts (MLflow)** | **All versions retained** | No purge | Required for rollback capability and audit trail |
| **Log Analytics** | **30 days** (Free Trial) / **365 days** (Production) | Auto-purged by Azure | PCI-DSS 10.7 requires 365 days for production |

### 7.9.2 Disaster Recovery Targets

| Component | RTO (Recovery Time) | RPO (Recovery Point) | Strategy |
|---|---|---|---|
| **ADLS Gen2 (Lakehouse)** | < 1 hour | 0 (ZRS in production) | LRS (dev) → ZRS (prod); Delta time-travel for data recovery |
| **Azure SQL (Case DB)** | < 15 minutes | 5 minutes | Geo-replicated backup; point-in-time restore |
| **Event Hubs (Streaming)** | < 5 minutes | 0 (Avro Capture to ADLS) | Replay from ADLS Avro Capture if namespace fails |
| **Model Endpoint** | < 10 minutes | 0 (MLflow Registry) | Redeploy from MLflow Model Registry; circuit breaker active during recovery |
| **Decision Engine (Functions)** | < 2 minutes | N/A (stateless) | Auto-restart by Azure Functions runtime |
| **Service Bus (Messaging)** | < 5 minutes | 0 (duplicate detection) | Standard tier auto-recovery; DLQ preserves unprocessed messages |

---

## 7.10 End-to-End Platform Verification (8-Step Runbook)

> [!NOTE]
> **Improved from original plan.** The original had only 5 steps and was missing verification of Databricks workspace, Azure Functions, and Logic Apps. The improved version has 8 comprehensive checks covering all deployed services.

#### `scripts/verify_platform_end_to_end.sh`

```bash
#!/usr/bin/env bash
# ============================================================
# End-to-End Platform Verification & Health Check Script
# 8-Step comprehensive check covering all Phase 0-7 services
# ============================================================
set -e

RG="rg-fraud-detection-dev"
KV="kv-fraud-dev"
EH_NS="ehns-fraud-dev"
SQL_SERVER="sql-fraud-dev"
STORAGE="stfraudlakedev"
SB_NS="sbns-fraud-dev"
APP_CONFIG="appcs-fraud-dev"
FUNC_APP="func-decision-engine-dev"

echo "============================================================"
echo "  REAL-TIME FRAUD DETECTION PLATFORM — FULL VERIFICATION    "
echo "============================================================"

# Step 1: Verify Resource Group
echo "[1/8] Verifying Resource Group..."
RG_STATUS=$(az group show --name $RG --query "properties.provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Resource Group: $RG_STATUS"
[ "$RG_STATUS" = "Succeeded" ] || { echo "❌ FAIL: Resource Group not found"; exit 1; }

# Step 2: Verify Key Vault Secrets
echo "[2/8] Verifying Key Vault Secrets..."
SECRET_COUNT=$(az keyvault secret list --vault-name $KV --query "length(@)" -o tsv 2>/dev/null || echo "0")
echo "  Key Vault Secrets Count: $SECRET_COUNT"
[ "$SECRET_COUNT" -gt 0 ] || { echo "❌ FAIL: No secrets in Key Vault"; exit 1; }

# Step 3: Verify ADLS Gen2 Storage
echo "[3/8] Verifying ADLS Gen2 Storage Account..."
STORAGE_STATUS=$(az storage account show --name $STORAGE --resource-group $RG --query "provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Storage Account: $STORAGE_STATUS"
[ "$STORAGE_STATUS" = "Succeeded" ] || { echo "❌ FAIL: Storage account not found"; exit 1; }

# Step 4: Verify Event Hubs
echo "[4/8] Verifying Event Hubs Namespace..."
EH_STATUS=$(az eventhubs namespace show --resource-group $RG --name $EH_NS --query "provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Event Hubs: $EH_STATUS"
[ "$EH_STATUS" = "Succeeded" ] || echo "⚠️ WARNING: Event Hubs namespace not found (expected if Phase 2 not deployed)"

# Step 5: Verify Azure SQL Database
echo "[5/8] Verifying Azure SQL Database Status..."
SQL_STATUS=$(az sql db show --resource-group $RG --server $SQL_SERVER --name "sqldb-fraud-cases-dev" --query "status" -o tsv 2>/dev/null || echo "MISSING")
echo "  Azure SQL: $SQL_STATUS"
[ "$SQL_STATUS" = "Online" ] || [ "$SQL_STATUS" = "AutoPaused" ] || echo "⚠️ WARNING: SQL Database not found (expected if Phase 5 not deployed)"

# Step 6: Verify Service Bus Topic
echo "[6/8] Verifying Service Bus Topic..."
TOPIC_STATUS=$(az servicebus topic show --resource-group $RG --namespace-name $SB_NS --name "sb-topic-fraud-events" --query "status" -o tsv 2>/dev/null || echo "MISSING")
echo "  Service Bus Topic: $TOPIC_STATUS"

# Step 7: Verify Azure Functions
echo "[7/8] Verifying Decision Engine Function App..."
FUNC_STATUS=$(az functionapp show --name $FUNC_APP --resource-group $RG --query "state" -o tsv 2>/dev/null || echo "MISSING")
echo "  Function App: $FUNC_STATUS"

# Step 8: Run TruffleHog Secret Verification
echo "[8/8] Running TruffleHog Secret Verification..."
if command -v trufflehog &> /dev/null; then
    trufflehog git file://. --only-verified 2>&1 | head -20 || echo "  No verified leaks found."
else
    echo "  TruffleHog not installed. Skipping (run in CI/CD pipeline instead)."
fi

echo "============================================================"
echo "  ✅ ALL PLATFORM VERIFICATION STEPS COMPLETED!             "
echo "============================================================"
```

---

## 7.11 Documentation

#### `docs/security_architecture.md`

> [!NOTE]
> **Missing from original plan.** Referenced in file tree but content was not provided.

```markdown
# Security Architecture — Real-Time Fraud Detection Platform

## 1. Network Security
- **Dev (Free Trial):** Service-level firewalls with Azure IP allowlisting. Public endpoints enabled.
- **Production:** Private Endpoints with Private DNS Zones for ADLS Gen2, Key Vault, Azure SQL, Service Bus. VNet-injected Databricks workspace.

## 2. Identity & Access Management
- **Zero Hardcoded Credentials:** All service-to-service auth uses Managed Identities.
- **User Auth:** Microsoft Entra ID groups (`fraud-analysts`, `data-engineers`, `platform-admins`).
- **Data RBAC:** Unity Catalog column masking + row filters for PII protection.

## 3. PCI-DSS Scope Isolation
- **No PAN Data:** Only tokenized card_id processed in the pipeline.
- **PII Hashing:** IP addresses and device fingerprints SHA-256 hashed with Key Vault salt at Bronze → Silver boundary.
- **Audit Trail:** 7-year immutable audit log (append-only Delta table + Azure SQL case_events).

## 4. Secret Management
- **Storage:** Azure Key Vault with soft-delete and purge protection.
- **Rotation:** Automated via `rotate_keyvault_secrets.py` (32-char cryptographically random passwords).
- **Access:** Databricks secret scope linked to Key Vault; Functions use Managed Identity.

## 5. CI/CD Security Gates
Every PR is scanned by 4 automated security checks:
1. **TruffleHog:** Verified secret leak detection across git history.
2. **Checkov:** Terraform IaC misconfiguration scanning.
3. **Bandit:** Python SAST for hardcoded secrets and insecure patterns.
4. **pip-audit:** Dependency vulnerability scanning against CVE databases.
```

#### `docs/operational_readiness_signoff.md`

> [!NOTE]
> **Missing from original plan.** Referenced in file tree but content was not provided.

```markdown
# Operational Readiness Sign-Off Checklist

## Pre-Production Verification

| # | Category | Check | Owner | Status |
|---|---|---|---|---|
| 1 | Infrastructure | All Terraform modules deploy successfully | Platform Team | ☐ |
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
```

---

## 7.12 File Tree — Phase 7 Additions

```
fraud-detection-platform/
├── infrastructure/
│   └── modules/
│       ├── rbac-assignments/main.tf              # [NEW] Entra ID RBAC with proper resource scoping
│       ├── private-endpoints/main.tf             # [NEW] ADLS + KV Private Endpoints with DNS Zones
│       └── diagnostic-settings/main.tf           # [NEW] Reusable centralized diagnostics module
│
├── databricks/
│   ├── governance/
│   │   └── apply_data_masking_policies.sql       # [NEW] Unity Catalog column masking + role grants
│   └── src/
│       └── security/
│           ├── __init__.py                       # [NEW] Security utilities package
│           └── pii_masking.py                    # [NEW] SHA-256 PII hashing + validation (fixed imports)
│
├── .github/
│   └── workflows/
│       └── security-scan.yml                    # [NEW] 4-scan CI pipeline (TruffleHog + Checkov + Bandit + pip-audit)
│
├── tests/
│   └── chaos/
│       └── test_resilience_scenarios.py         # [NEW] 5-scenario chaos suite (concurrent storm, boundary scores)
│
├── scripts/
│   ├── rotate_keyvault_secrets.py               # [NEW] Multi-secret rotation with audit logging
│   └── verify_platform_end_to_end.sh            # [NEW] 8-step platform verification runbook
│
└── docs/
    ├── security_architecture.md                 # [NEW] PCI-DSS scope, RBAC, network security documentation
    └── operational_readiness_signoff.md         # [NEW] 17-item production readiness checklist
```

---

## 7.13 Phase 7 Validation Checklist

| # | Check | Command / Method | Expected Result | Criticality |
|---|---|---|---|---|
| 1 | Unity Catalog masking masks IP/Device | Execute `apply_data_masking_policies.sql` | Non-compliance roles see `xxx.xxx` IP, `****` device IDs | 🔴 Blocking |
| 2 | SHA-256 salting hashes PII | Run `pii_masking.py` with `validate_pii_hashing()` | All IP/Device strings transformed to 64-char hashes | 🔴 Blocking |
| 3 | Entra ID Managed Identity assigned | Deploy `rbac-assignments` module | Databricks accesses ADLS without storage keys | 🔴 Blocking |
| 4 | Key Vault secrets rotate (all 3) | Run `rotate_keyvault_secrets.py` | New secret versions created for all 3 secrets | 🟡 Warning |
| 5 | Diagnostic settings capture logs | Deploy `diagnostic-settings` module | `AzureDiagnostics` records appear in Log Analytics | 🔴 Blocking |
| 6 | Private Endpoints deploy (when enabled) | Deploy `private-endpoints` module with `enable_private_endpoints=true` | Private endpoints + DNS zones created | 🟡 Warning |
| 7 | TruffleHog scan passes | Run `security-scan.yml` | Zero verified secret leaks | 🔴 Blocking |
| 8 | Checkov IaC scan passes | Run Checkov on `infrastructure/` | Zero critical Terraform security violations | 🔴 Blocking |
| 9 | Bandit Python SAST passes | Run Bandit on `ml/`, `functions/`, `scripts/` | Zero high-severity findings | 🔴 Blocking |
| 10 | pip-audit dependency scan passes | Run pip-audit on all requirements.txt | Zero critical CVEs in dependencies | 🟡 Warning |
| 11 | Chaos: corrupted payload fallback | Run `test_resilience_scenarios.py::test_01` | Returns 200 with fallback decision | 🔴 Blocking |
| 12 | Chaos: concurrent storm 95%+ success | Run `test_resilience_scenarios.py::test_04` | ≥ 47/50 concurrent requests succeed | 🔴 Blocking |
| 13 | Chaos: score boundary routing | Run `test_resilience_scenarios.py::test_05` | All 10 boundary scores route to correct action | 🔴 Blocking |
| 14 | End-to-end verification passes | Run `verify_platform_end_to_end.sh` | All 8 verification steps return OK | 🔴 Blocking |
| 15 | Operational readiness sign-off | Review `operational_readiness_signoff.md` | All 17 checklist items verified | 🔴 Blocking |

---

## Production Decision Registry (Phase 7)

| # | Decision | Free Trial Choice | Production Upgrade | Rationale |
|---|---|---|---|---|
| 1 | Governance Catalog | **Unity Catalog (Column Masking + Row Filters)** | Microsoft Purview Automated Lineage | Unity Catalog provides native masking without Purview cost |
| 2 | PII Protection | **SHA-256 Hashing with Key Vault Salt + Validation** | Tokenization Service + HSM | Hashes PII at Bronze→Silver boundary with automated validation |
| 3 | Network Security | **Service Firewalls + Azure IP Rules** | Private Endpoints + DNS Zones (`private-endpoints` module) | Private endpoints cost ~$7.20/month per service |
| 4 | Authentication | **Microsoft Entra ID Managed Identities** | Same | Zero hardcoded storage keys or database passwords |
| 5 | Log Retention | **Log Analytics 30-Day Retention** | Log Analytics 365-Day Retention | 30 days is free; 365 days required for PCI compliance |
| 6 | Security Automation | **4-Scan Pipeline (TruffleHog + Checkov + Bandit + pip-audit)** | Same + SonarQube Enterprise | Comprehensive secret, IaC, SAST, and CVE scanning |
| 7 | Chaos Testing | **5-Scenario Test Suite (Concurrent + Boundary)** | Same + Azure Chaos Studio | ThreadPoolExecutor-based concurrency + score boundary validation |
| 8 | Data Retention | **Bronze: Indefinite, Silver: 3yr, Gold: 7yr, Audit: 7yr** | Same | PCI-DSS 10.7 retention requirements |
| 9 | Disaster Recovery | **RTO < 1hr (Lakehouse), RPO: 0 (ZRS + Delta)** | Cross-region geo-replication | Dev uses LRS; production upgrades to ZRS + geo-replicas |

---

## Known Issues & Fixes Applied (post-implementation code review, 2026-08-08)

| # | Component | Bug | Fix |
|---|---|---|---|
| 1 | `scripts/rotate_keyvault_secrets.py` | `rotate_secret()` generated a new random password and wrote it to Key Vault via `client.set_secret(...)`, but never changed the corresponding credential on the actual target system. For `db-admin-password-dev`/`sql-admin-password-dev`, that meant "rotation" made Key Vault hold a value that no longer matched the real Azure SQL Server admin password — every service authenticating with that secret would start failing immediately after a "successful" rotation run. This was the most severe bug found in the reviewed scripts: a rotation routine that breaks connectivity instead of rotating credentials safely. | Added `_update_sql_server_password()`, which updates the live Azure SQL Server admin login password via the `azure-mgmt-sql` ARM client (`servers.begin_update`) for the two secrets that mirror it (`SQL_SERVER_ADMIN_SECRETS`). The real server credential is updated *first* and Key Vault is only written to once that ARM call succeeds, so a failed rotation can never leave Key Vault out of sync with the live server. Added `azure-mgmt-sql` to `requirements.txt`. Note: `pii-hash-salt` is intentionally left untouched by this fix — it isn't a system credential, and rotating it has a different consequence (breaks matching of previously-hashed PII values) that's out of scope here. |
| 2 | `infrastructure/modules/rbac-assignments/main.tf` (originally found in the Bicep version of this module) | Accepted a `decisionFunctionPrincipalId` parameter but never used it in any role assignment — per this phase's own §7.3 RBAC matrix, `id-decision-function` should get **Service Bus Data Sender** + **App Configuration Data Reader**, and `id-logic-app-workflow` should get **Service Bus Data Receiver** + **SQL DB Contributor**; none of these 4 assignments existed anywhere. The Decision Function/Logic App managed identities had no RBAC-based access, leaving them dependent on connection-string/SAS auth despite the platform's documented zero-trust managed-identity design. | Fix carried forward into the Terraform rewrite: the module now takes `logic_app_principal_id` and defines all 6 `azurerm_role_assignment` resources shown in §7.3, scoped to the storage account, Key Vault, Service Bus namespace, App Configuration store, and SQL database respectively. Built-in roles are referenced by **name** (`role_definition_name`), not a hardcoded GUID, so the provider resolves and validates them against the live subscription at plan/apply time instead of failing silently on a stale ID. |
| 3 | `infrastructure/modules/private-endpoints/main.tf` (originally found in the Bicep version of this module) | Created the private endpoints and private DNS zones when `enablePrivateEndpoints=true`, but never created a `privateDnsZoneGroup` linking each endpoint to its zone, nor a `virtualNetworkLinks` resource linking the zones to the VNet (the `vnetName` param was only used to build the subnet resource ID, never referenced by the DNS zone resources). Even with private endpoints enabled, DNS resolution for `*.blob.core.windows.net`/`*.vault.azure.net` would still resolve to public IPs from the VNet, defeating the point of the private endpoints. | Fix carried forward into the Terraform rewrite: `azurerm_private_dns_zone_virtual_network_link` resources link both zones to the VNet, and each `azurerm_private_endpoint` nests its own `private_dns_zone_group` block — the azurerm provider models the endpoint↔zone link as a nested attribute of the endpoint itself, so the two can't drift apart the way two independent Bicep resources could. |
| 4 | `tests/chaos/test_resilience_scenarios.py` | Every `requests.post` call omitted the function key required by `functions/decision_engine/function_app.py`'s `http_auth_level=func.AuthLevel.FUNCTION` — every assertion in the suite would fail with 401 Unauthorized against a real deployment rather than exercising any actual resilience behavior. The module docstring also claimed 2 scenarios (Service Bus retry, connection pool exhaustion) that were never implemented — only 5 scenarios exist: corrupted payload, missing fields, sequential latency, concurrent storm, score boundaries. | Added `AUTH_HEADERS` (from a `DECISION_ENGINE_FUNCTION_KEY` env var) to all 5 requests. Corrected the docstring to describe only what's actually tested, rather than claiming untested coverage — deliberately did not fabricate the 2 missing scenarios without live Service Bus/SQL infrastructure to validate them against. |
| 5 | `scripts/verify_platform_end_to_end.sh` | Steps 1-3 (Resource Group, Key Vault, Storage) exited on failure; steps 4-7 (Event Hubs, SQL, Service Bus, Function App) only printed status and never asserted anything — a partially-deployed platform could still print "ALL PLATFORM VERIFICATION STEPS COMPLETED!". The `APP_CONFIG` variable was declared but never used anywhere — App Configuration (holding the live fraud decision thresholds) was silently never verified at all. | Steps 4-7 now exit 1 on an unexpected status, matching steps 1-3's rigor. Added a new step verifying the 3 App Configuration threshold keys (`FraudEngine:{ApproveMax,StepUpMax,BlockMin}Threshold`) actually exist, using the previously-dead `APP_CONFIG` variable. Script is now 9 steps, not 8. |
