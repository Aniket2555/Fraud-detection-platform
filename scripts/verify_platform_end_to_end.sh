#!/usr/bin/env bash
# ============================================================
# End-to-End Platform Verification & Health Check Script
# 9-Step comprehensive check covering all Phase 0-7 services
#
# Production fix applied: steps 4-7 (Event Hubs, SQL, Service Bus,
# Function App) previously only printed status and never exited on
# failure, unlike steps 1-3 -- a partially-deployed platform could
# still print "ALL PLATFORM VERIFICATION STEPS COMPLETED!". Also added
# a real App Configuration check (the APP_CONFIG variable was declared
# but never used -- App Config holds the live fraud decision thresholds
# and was silently never verified at all).
#
# Two more real bugs fixed against the actual live deployment (see
# docs/execution-log/*.md): KV was "kv-fraud-dev" -- Key Vault names are
# globally unique, and the real vault has a random suffix
# ("kv-fraud-dev-4th9"). FUNC_APP was "func-decision-engine-dev" -- the
# real Phase 5 Function App is "func-fraud-decision-dev" (same wrong
# name already found and fixed in tests/chaos/test_resilience_scenarios.py).
# ============================================================
set -e

RG="rg-fraud-detection-dev"
KV="kv-fraud-dev-4th9"
EH_NS="ehns-fraud-dev"
SQL_SERVER="sql-fraud-dev"
STORAGE="stfraudlakedev"
SB_NS="sbns-fraud-dev"
APP_CONFIG="appcs-fraud-dev"
FUNC_APP="func-fraud-decision-dev"

echo "============================================================"
echo "  REAL-TIME FRAUD DETECTION PLATFORM — FULL VERIFICATION    "
echo "============================================================"

# Step 1: Verify Resource Group
echo "[1/9] Verifying Resource Group..."
RG_STATUS=$(az group show --name $RG --query "properties.provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Resource Group: $RG_STATUS"
[ "$RG_STATUS" = "Succeeded" ] || { echo "❌ FAIL: Resource Group not found"; exit 1; }

# Step 2: Verify Key Vault Secrets
echo "[2/9] Verifying Key Vault Secrets..."
SECRET_COUNT=$(az keyvault secret list --vault-name $KV --query "length(@)" -o tsv 2>/dev/null || echo "0")
echo "  Key Vault Secrets Count: $SECRET_COUNT"
[ "$SECRET_COUNT" -gt 0 ] || { echo "❌ FAIL: No secrets in Key Vault"; exit 1; }

# Step 3: Verify ADLS Gen2 Storage
echo "[3/9] Verifying ADLS Gen2 Storage Account..."
STORAGE_STATUS=$(az storage account show --name $STORAGE --resource-group $RG --query "provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Storage Account: $STORAGE_STATUS"
[ "$STORAGE_STATUS" = "Succeeded" ] || { echo "❌ FAIL: Storage account not found"; exit 1; }

# Step 4: Verify Event Hubs
echo "[4/9] Verifying Event Hubs Namespace..."
EH_STATUS=$(az eventhubs namespace show --resource-group $RG --name $EH_NS --query "provisioningState" -o tsv 2>/dev/null || echo "MISSING")
echo "  Event Hubs: $EH_STATUS"
[ "$EH_STATUS" = "Succeeded" ] || { echo "❌ FAIL: Event Hubs namespace not found"; exit 1; }

# Step 5: Verify Azure SQL Database
# "Paused" is a legitimate healthy state, not a failure -- the database is
# GP_S_Gen5_1 serverless with a deliberate 60-min auto-pause (Phase 5 cost
# saving; see infrastructure/modules/azure-sql/main.tf); it auto-resumes on
# the next real query. A prior version of this check only accepted "Online",
# which would false-fail any time the platform had simply been idle.
echo "[5/9] Verifying Azure SQL Database Status..."
SQL_STATUS=$(az sql db show --resource-group $RG --server $SQL_SERVER --name "sqldb-fraud-cases-dev" --query "status" -o tsv 2>/dev/null || echo "MISSING")
echo "  Azure SQL: $SQL_STATUS"
[ "$SQL_STATUS" = "Online" ] || [ "$SQL_STATUS" = "Paused" ] || { echo "❌ FAIL: Azure SQL database not online or paused"; exit 1; }

# Step 6: Verify Service Bus Topic
echo "[6/9] Verifying Service Bus Topic..."
TOPIC_STATUS=$(az servicebus topic show --resource-group $RG --namespace-name $SB_NS --name "sb-topic-fraud-events" --query "status" -o tsv 2>/dev/null || echo "MISSING")
echo "  Service Bus Topic: $TOPIC_STATUS"
[ "$TOPIC_STATUS" = "Active" ] || { echo "❌ FAIL: Service Bus topic not active"; exit 1; }

# Step 7: Verify Azure Functions
echo "[7/9] Verifying Decision Engine Function App..."
FUNC_STATUS=$(az functionapp show --name $FUNC_APP --resource-group $RG --query "state" -o tsv 2>/dev/null || echo "MISSING")
echo "  Function App: $FUNC_STATUS"
[ "$FUNC_STATUS" = "Running" ] || { echo "❌ FAIL: Decision engine function app not running"; exit 1; }

# Step 8: Verify App Configuration holds the live decision thresholds
echo "[8/9] Verifying App Configuration Fraud Thresholds..."
for KEY in "FraudEngine:ApproveMaxThreshold" "FraudEngine:StepUpMaxThreshold" "FraudEngine:BlockMinThreshold"; do
    VALUE=$(az appconfig kv show --name $APP_CONFIG --key "$KEY" --query "value" -o tsv 2>/dev/null || echo "MISSING")
    echo "  $KEY = $VALUE"
    [ "$VALUE" != "MISSING" ] || { echo "❌ FAIL: App Configuration key '$KEY' not found"; exit 1; }
done

# Step 9: Run TruffleHog Secret Verification
echo "[9/9] Running TruffleHog Secret Verification..."
if command -v trufflehog &> /dev/null; then
    trufflehog git file://. --only-verified 2>&1 | head -20 || echo "  No verified leaks found."
else
    echo "  TruffleHog not installed. Skipping (run in CI/CD pipeline instead)."
fi

echo "============================================================"
echo "  ✅ ALL PLATFORM VERIFICATION STEPS COMPLETED!             "
echo "============================================================"
