# Azure Free Trial Budget & Cost Management Guide

> [!IMPORTANT]
> The Azure Free Trial grants **$200 credit for 30 days**. This guide documents essential rules to keep overall project spend under $100 while completing all 7 implementation phases.

---

## Top 5 Cost Drivers

| Resource | Uncontrolled Cost | Mitigated Cost | Action Required |
|---|---|---|---|
| **Databricks Compute** | $15–30/day (if left running) | $1–3/day | Set 20-min auto-termination on all cluster policies |
| **Azure ML Endpoints** | $3–5/day continuous | $0 (in Phase 0–2) | Deferred to Phase 3; use Databricks MLflow locally |
| **Private Endpoints** | $7.20/month per endpoint | $0 | Use public access + Service Firewalls for Free Trial |
| **Azure SQL Database** | $15–30/month provisioned | ~$0/month | Serverless SKU (`GP_S_Gen5_1`) with 60-min auto-pause |
| **Cosmos DB** | $25+/month provisioned RU | <$2/month | Serverless SKU or strict 400 RU/s cap |

---

## Daily Budget Checklist

1. **Before Starting Work:**
   - Verify cluster auto-termination is set to **20 minutes**.
   - Use single-node clusters (`Standard_DS3_v2`, 0 workers) for initial notebook runs.

2. **After Finishing Work:**
   - Manually terminate all active Databricks clusters.
   - Verify no jobs are left running in an infinite loop.

3. **Monitoring Azure Portal:**
   - Check **Cost Management → Cost Analysis** daily.
   - Ensure spending stays under **$5/day**.

---

## Budget Alerts Setup

Run the following Azure CLI commands to configure budget threshold alerts:

```bash
# Set a $50 budget alert
az consumption budget create \
  --budget-name "FreeTrialBudget" \
  --amount 100 \
  --time-grain Monthly \
  --start-date $(date +%Y-%m-01) \
  --end-date $(date -d "+1 year" +%Y-%m-01) \
  --resource-group "rg-fraud-detection-dev"
```
