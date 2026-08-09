# Security Architecture — Real-Time Fraud Detection Platform

## 1. Network Security
- **Dev (Free Trial):** Service-level firewalls with Azure IP allowlisting. Public endpoints enabled.
- **Production:** Private Endpoints with Private DNS Zones for ADLS Gen2, Key Vault, Azure SQL, Service Bus (`private-endpoints.bicep`). VNet-injected Databricks workspace.

## 2. Identity & Access Management
- **Zero Hardcoded Credentials:** All service-to-service auth uses Microsoft Entra ID Managed Identities (`rbac-assignments.bicep`).
- **User Auth:** Microsoft Entra ID groups (`fraud-analysts`, `data-engineers`, `platform-admins`, `ml-engineers`).
- **Data RBAC:** Unity Catalog column masking (`apply_data_masking_policies.sql`) + row filters for PII protection.

## 3. PCI-DSS Scope Isolation
- **No PAN Data:** Only tokenized `card_id` processed in the pipeline.
- **PII Hashing:** IP addresses and device fingerprints SHA-256 hashed with Key Vault salt at Bronze → Silver boundary (`pii_masking.py`).
- **Audit Trail:** 7-year immutable audit log (append-only Delta table + Azure SQL `case_events`).

## 4. Secret Management
- **Storage:** Azure Key Vault with soft-delete and purge protection.
- **Rotation:** Automated via `rotate_keyvault_secrets.py` (32-char cryptographically random passwords).
- **Access:** Databricks secret scope linked to Key Vault; Functions use Managed Identity.

## 5. CI/CD Security Gates
Every PR is scanned by 4 automated security checks (`security-scan.yml`):
1. **TruffleHog:** Verified secret leak detection across git history.
2. **Checkov:** Bicep IaC misconfiguration scanning.
3. **Bandit:** Python SAST for hardcoded secrets and insecure patterns.
4. **pip-audit:** Dependency vulnerability scanning against CVE databases.
