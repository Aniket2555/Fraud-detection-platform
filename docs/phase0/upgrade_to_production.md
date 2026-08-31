# Phase 0 — Production Upgrade Runbook

This document describes the architectural and configuration changes required when transitioning this platform from the **Azure Free Trial** setup to an enterprise-grade **Pay-As-You-Go / Production** environment.

---

## Upgrade Steps Matrix

| Component | Free Trial Architecture | Production Upgrade Target | Actions Required |
|---|---|---|---|
| **Networking** | Public endpoints + Service Firewalls | VNet Injection + Private Endpoints | Add a `vnet` Terraform module (6 subnets, 9 Private DNS zones) and set `enable_private_endpoints = true` in `environments/prod.tfvars` — `infrastructure/modules/private-endpoints` already exists and just needs a real `vnet_id`/`private_endpoint_subnet_id` |
| **Databricks Tier** | Premium Trial / Standard | Premium (Permanent) | Enable Unity Catalog across dev/staging/prod workspaces with Metastore admin binding |
| **Databricks Compute** | Single-Node `Standard_DS3_v2` | Multi-Node Autoscaling (`Standard_DS4_v2` / `Standard_E8ds_v5`) | Update cluster policies to permit 2–8 workers; enable Photon engine for batch ETL |
| **Storage (ADLS)** | Standard LRS | Standard ZRS (Zone Redundant) | Re-provision ADLS Gen2 with ZRS redundancy and 30-day soft delete |
| **Key Vault** | Standard SKU | Premium SKU (HSM-backed) | Upgrade SKU for HSM key protection and enable purge protection |
| **Model Serving** | Databricks MLflow PyFunc | Azure ML Managed Online Endpoints | Add an `azureml-workspace` Terraform module (`azurerm_machine_learning_workspace`) + Azure ML Online Endpoints (`Standard_DS3_v2` autoscale pool) — no such module exists yet in either the old Bicep or the new Terraform config |
| **Azure SQL** | Serverless `GP_S_Gen5_1` (Auto-pause) | Provisioned General Purpose / Business Critical | Disable auto-pause, enable ZRS, and configure Read Replicas |
| **Service Bus** | Standard Tier | Premium Tier | Upgrade Service Bus to Premium Tier for VNet integration and dedicated capacity |
| **App Configuration**| Free Tier (1,000 req/day) | Standard Tier | Upgrade to Standard SKU for unlimited requests and private endpoints |
| **CI/CD Security** | Client Secrets | OIDC Federated Credentials | Configure Azure AD Workload Identity Federation for GitHub Actions workflows |
| **Environment Parity**| Single `dev` environment | `dev`, `staging`, `prod` isolated subscriptions | Deploy multi-subscription RG hierarchy with parameter files for staging/prod |
