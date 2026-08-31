# Free Trial / dev environment variables.
# Replaces infrastructure/modules/parameters/dev.parameters.json from the Bicep version.
#
# Deliberately NOT included here: sql_admin_password. It's a sensitive
# variable with no default -- supply it out-of-band at apply time, e.g.:
#   export TF_VAR_sql_admin_password="..."
#   terraform apply -var-file=environments/dev.tfvars
# Never commit a real password into a .tfvars file.

environment        = "dev"
location           = "centralindia"
owner_email        = "aniket.tiwari.ys@gmail.com"
deployer_object_id = "e47f7602-2c61-4c5a-bdaa-4eccbf6ffb33"

enable_private_endpoints = false

# Wired to the real func-fraud-decision-dev Function App's system-assigned
# managed identity (see docs/execution-log/07-decision-engine.md) so the
# rbac-assignments module's Service Bus Data Sender / App Configuration
# Data Reader grants actually apply to something real, per the Phase 7
# §7.3 RBAC matrix. logic_app_principal_id stays unset -- no Logic App has
# been deployed (documented gap, see 07-decision-engine.md).
decision_function_principal_id = "5fde6a4d-66b3-4484-a034-b0282e3ae316"
