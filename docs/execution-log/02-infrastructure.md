# 02 — Infrastructure (Terraform + CI secrets)

**Starting state:** zero Azure resources existed anywhere. `terraform apply`
had never been run. `dev.tfvars` had placeholder values
(`your-email@example.com`, `YOUR-OBJECT-ID`).

**End state:** 64 resources live in `rg-fraud-detection-dev`; CI service
principal created; all 11 GitHub Actions secrets set.

## Tools installed

```powershell
winget install -e --id Microsoft.AzureCLI
winget install -e --id GitHub.cli
```

Terraform (v1.15.8) was already installed.

## Windows/Git-Bash gotcha you'll hit repeatedly

Git Bash mangles two kinds of arguments unless handled explicitly:

- **Paths starting with `/`** (e.g. `/subscriptions/...` for `az` `--ids`
  or REST URLs) get auto-converted to Windows paths. Fix: prefix the
  command with `MSYS_NO_PATHCONV=1`.
- **`PATH` entries in `C:/...` form** get corrupted because Git Bash's `:`
  separator collides with the drive letter's colon. Always use the Unix
  mount form: `/c/Program Files/...`, not `C:/Program Files/...`.

## Logging in

```bash
# Azure CLI — interactive browser login, must be run by a human in their
# own terminal (an agent can't complete a browser OAuth flow)
az login

# GitHub CLI — same constraint
gh auth login
```

## Terraform: bootstrap the remote state backend (one-time)

```bash
cd infrastructure/bootstrap
terraform init
terraform plan -var="environment=dev" -out=bootstrap.tfplan
terraform apply bootstrap.tfplan
# Note the printed storage_account_name (was: sttfstatedevmvm94i)
```

Fill in `infrastructure/backend-dev.conf` (gitignored) with the printed
values:
```
resource_group_name  = "rg-tfstate-fraud-dev"
storage_account_name = "sttfstatedevmvm94i"
container_name        = "tfstate"
key                   = "fraud-detection-dev.tfstate"
```

## Terraform: fill in `dev.tfvars`

```bash
az ad signed-in-user show --query id -o tsv   # your Azure AD object ID
```

Edit `infrastructure/environments/dev.tfvars`:
```hcl
owner_email        = "<your-email>"
deployer_object_id = "<object id from above>"
```

## Terraform: generate the SQL admin password

Never commit this. It's supplied via env var at apply time.
```bash
.venv/Scripts/python.exe -c "
import secrets, string
alphabet = string.ascii_letters + string.digits
pw = ''.join(secrets.choice(alphabet) for _ in range(24)) + '!' + secrets.choice(string.digits) + '#'
print(pw)
" > infrastructure/.sql_admin_password.local   # gitignored
export TF_VAR_sql_admin_password="$(cat infrastructure/.sql_admin_password.local)"
```

## Terraform: init, plan, apply

```bash
export PATH="$PATH:/c/Program Files/Microsoft SDKs/Azure/CLI2/wbin"
cd infrastructure
terraform init -input=false -backend-config=backend-dev.conf
terraform plan -input=false -var-file=environments/dev.tfvars -out=dev.tfplan
terraform apply -input=false dev.tfplan
```

This creates 59 resources on the first clean apply. **Real, ongoing cost:
~$20-25/month base** (Event Hubs Standard + Service Bus Standard), plus
usage-based charges only when Databricks clusters actually run or
Cosmos DB/SQL are queried (both serverless/auto-pause).

### Bugs hit during apply, and their fixes

All already fixed in the working tree; documented here so you understand
why the code looks the way it does.

**1. App Configuration key writes hung for 45 minutes, then failed.**
Root cause: writing App Config key-values requires the "App Configuration
Data Owner" data-plane RBAC role — not covered by subscription Owner or
any ARM-level permission. This is a genuine Azure quirk (unlike Key Vault,
App Config key-values aren't governed by resource-level RBAC alone). Fix
(one-time, per environment, after the store exists):
```bash
az role assignment create \
  --assignee <your-object-id> \
  --role "App Configuration Data Owner" \
  --scope "/subscriptions/<sub>/resourceGroups/rg-fraud-detection-dev/providers/Microsoft.AppConfiguration/configurationStores/appcs-fraud-dev"
```
If `az role assignment create` itself fails with a `MissingSubscription`
error (a real CLI bug we hit), use `az rest` directly instead:
```bash
GUID=$(python -c "import uuid; print(uuid.uuid4())")
ROLE_ID=$(az role definition list --name "App Configuration Data Owner" --query "[0].id" -o tsv)
SCOPE="/subscriptions/<sub>/resourceGroups/rg-fraud-detection-dev/providers/Microsoft.AppConfiguration/configurationStores/appcs-fraud-dev"
az rest --method put \
  --url "https://management.azure.com${SCOPE}/providers/Microsoft.Authorization/roleAssignments/${GUID}?api-version=2022-04-01" \
  --body "{\"properties\":{\"roleDefinitionId\":\"${ROLE_ID}\",\"principalId\":\"<your-object-id>\",\"principalType\":\"User\"}}"
```

**2. Key Vault and Cosmos DB names collided globally.** Both resource
types have DNS-style globally-unique names across *all* Azure tenants —
`kv-fraud-dev` and `cosmos-fraud-dev` were already taken by someone else.
Fixed in `infrastructure/modules/key-vault/main.tf` and
`infrastructure/modules/cosmos-db/main.tf` by adding a `random_string`
suffix (same pattern the bootstrap config already used for the tfstate
storage account). Real names now: `kv-fraud-dev-4th9`, `cosmos-fraud-dev-604t`.

**3. `rbac-assignments` module tried to grant roles to identities that
don't exist yet** (Decision Function, Logic App — both Phase 5; and
Databricks's own storage identity, which doesn't populate on
Unity-Catalog-enabled workspaces). Fixed with `count` guards in
`infrastructure/modules/rbac-assignments/main.tf` — each assignment is
skipped when its principal ID is null/empty.

**4. `diagnostic-settings` module hardcoded log/metric categories that
Storage accounts and Databricks workspaces don't support.** Fixed by
making both toggleable (`enable_logs`, `enable_metrics` variables) in
`infrastructure/modules/diagnostic-settings/main.tf`, with the calling
`for_each` in `infrastructure/main.tf` setting the right flags per
resource type.

### A cosmetic, permanent, safe-to-ignore diff

`terraform plan` will forever show a 4-resource diff (the storage
account's `network_rules` block + 3 diagnostic settings) — a known
azurerm provider quirk where Azure's API doesn't echo back certain
"default/allow" settings on refresh. Re-applying doesn't change real
behavior. Don't chase this.

## CI service principal + GitHub secrets

```bash
# Create SP scoped to just this project's resource group
az ad sp create-for-rbac --name "sp-fraud-detection-dev-ci" \
  --role Contributor \
  --scopes "/subscriptions/<sub>/resourceGroups/rg-fraud-detection-dev"
# (if the role-assignment part fails with the same MissingSubscription
#  bug as above, the SP itself is still created — reset its credentials
#  instead of recreating: az ad sp credential reset --id <appId>)
```

The SP also needs 3 narrow data-plane roles, for the same reasons as the
manual grants above — CI runs `terraform apply` too and needs to
actually write to these services:
```bash
# Repeat the az rest pattern from above for each of:
#   - Contributor on the resource group (control plane)
#   - Key Vault Administrator on the vault
#   - App Configuration Data Owner on the app config store
#   - Storage Blob Data Contributor on the *tfstate* storage account
```

Set the 11 GitHub Actions secrets (`infra-deploy.yml` reads these):
```bash
gh secret set AZURE_SUBSCRIPTION_ID --repo Aniket2555/Fraud-detection-platform --body "<sub>"
gh secret set AZURE_TENANT_ID --repo Aniket2555/Fraud-detection-platform --body "<tenant>"
gh secret set AZURE_CLIENT_ID --repo Aniket2555/Fraud-detection-platform --body "<sp app id>"
gh secret set AZURE_CLIENT_SECRET --repo Aniket2555/Fraud-detection-platform --body "<sp secret>"
gh secret set TFSTATE_RESOURCE_GROUP --repo Aniket2555/Fraud-detection-platform --body "rg-tfstate-fraud-dev"
gh secret set TFSTATE_STORAGE_ACCOUNT --repo Aniket2555/Fraud-detection-platform --body "sttfstatedevmvm94i"
gh secret set OWNER_EMAIL --repo Aniket2555/Fraud-detection-platform --body "<email>"
gh secret set DEPLOYER_OBJECT_ID --repo Aniket2555/Fraud-detection-platform --body "<your object id, NOT the SP's>"
gh secret set SQL_ADMIN_PASSWORD --repo Aniket2555/Fraud-detection-platform --body "$(cat infrastructure/.sql_admin_password.local)"
gh secret set DECISION_FUNCTION_PRINCIPAL_ID --repo Aniket2555/Fraud-detection-platform --body ""   # empty until Phase 5
gh secret set LOGIC_APP_PRINCIPAL_ID --repo Aniket2555/Fraud-detection-platform --body ""            # empty until Phase 5
```

**Why `DEPLOYER_OBJECT_ID` is your personal ID, not the SP's:** the
key-vault module's `deployer_object_id` variable drives a single tracked
`azurerm_role_assignment` resource. If CI used a different value than
your local `dev.tfvars`, every `terraform apply` (local vs. CI) would
fight over that resource's `principal_id` (which is `ForceNew` —
Terraform would destroy-and-recreate it, revoking access, every time).
Keeping both the same avoids drift. The SP's own Key Vault access was
granted separately, out-of-band, as one of the 3 data-plane roles above.

```bash
gh secret list --repo Aniket2555/Fraud-detection-platform   # verify
```

## Verifying the deployment

```bash
terraform plan -input=false -var-file=environments/dev.tfvars
# should show only the cosmetic network_rules/diagnostics diff (see above)
```
