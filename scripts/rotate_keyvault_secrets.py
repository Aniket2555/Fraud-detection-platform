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
from azure.mgmt.sql import SqlManagementClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secret_rotation")

# KEY_VAULT_NAME has no safe hardcodable default: Key Vault names are globally
# unique, so this environment's real vault has a random suffix
# ("kv-fraud-dev-4th9", not "kv-fraud-dev") -- same class of bug already found
# in scripts/store_eventhub_secrets.sh and databricks/workspace-setup/
# secret_scope_setup.sh (Phase 5). Resolved dynamically instead of guessing.
def _resolve_vault_name() -> str:
    env_override = os.environ.get("KEY_VAULT_NAME")
    if env_override:
        return env_override
    from azure.mgmt.resource.resources import ResourceManagementClient
    if not SUBSCRIPTION_ID:
        raise RuntimeError("Set KEY_VAULT_NAME or AZURE_SUBSCRIPTION_ID to resolve the vault.")
    credential = DefaultAzureCredential()
    client = ResourceManagementClient(credential, SUBSCRIPTION_ID)
    for res in client.resources.list_by_resource_group(
        SQL_RESOURCE_GROUP, filter="resourceType eq 'Microsoft.KeyVault/vaults'"
    ):
        return res.name
    raise RuntimeError(f"No Key Vault found in resource group {SQL_RESOURCE_GROUP!r}.")


ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
SQL_RESOURCE_GROUP = os.environ.get("SQL_RESOURCE_GROUP", f"rg-fraud-detection-{ENVIRONMENT}")
SQL_SERVER_NAME = os.environ.get("SQL_SERVER_NAME", f"sql-fraud-{ENVIRONMENT}")

VAULT_NAME = _resolve_vault_name()
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net"

# The original list ("db-admin-password-dev", "sql-admin-password-dev") named
# secrets that don't exist anywhere in this environment -- the real SQL
# credential lives in "azure-sql-jdbc-url", a full JDBC connection string
# with the password embedded (see docs/execution-log/08-mlops-loop.md),
# not a bare password. Running the original list would have rotated the
# live SQL Server password TWICE (once per mismatched name, each treated as
# a separate SQL_SERVER_ADMIN_SECRETS entry) while leaving the actual
# secret every real consumer (Databricks JDBC) reads completely unrotated
# and now out of sync with the live server password.
ROTATABLE_SECRETS = [
    "pii-hash-salt",
    "azure-sql-jdbc-url",
]

# Secrets that mirror a live credential on the Azure SQL Server. Rotating
# them must update the real server password too -- writing a fresh random
# value to Key Vault alone leaves the server's actual admin password
# unchanged, so every service reading the "rotated" secret would
# immediately fail to authenticate.
SQL_SERVER_ADMIN_SECRETS = {"azure-sql-jdbc-url"}
SQL_ADMIN_LOGIN = os.environ.get("SQL_ADMIN_LOGIN", "fraudsqladmin")
SQL_DATABASE_NAME = os.environ.get("SQL_DATABASE_NAME", f"sqldb-fraud-cases-{ENVIRONMENT}")


def _build_jdbc_url(password: str) -> str:
    return (
        f"jdbc:sqlserver://{SQL_SERVER_NAME}.database.windows.net:1433;"
        f"database={SQL_DATABASE_NAME};user={SQL_ADMIN_LOGIN};password={password};"
        f"encrypt=true;trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    )


def generate_secure_password(length: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def _update_sql_server_password(new_value: str) -> None:
    """Updates the live Azure SQL Server admin login password via ARM."""
    if not SUBSCRIPTION_ID:
        raise RuntimeError(
            "AZURE_SUBSCRIPTION_ID is not set -- refusing to rotate a SQL admin "
            "secret without being able to update the real server credential."
        )
    credential = DefaultAzureCredential()
    sql_client = SqlManagementClient(credential, SUBSCRIPTION_ID)
    # A raw dict here deserializes server-side as a generic ResourceDefinition
    # that doesn't recognize administrator_login_password at all (verified
    # against the live API: 400 InvalidRequestContent) -- azure-mgmt-sql 4.x
    # needs the actual ServerUpdate model, not a bare dict.
    from azure.mgmt.sql.models import ServerUpdate
    poller = sql_client.servers.begin_update(
        resource_group_name=SQL_RESOURCE_GROUP,
        server_name=SQL_SERVER_NAME,
        parameters=ServerUpdate(administrator_login_password=new_value),
    )
    poller.result()  # block until the server confirms the new password took effect


def rotate_secret(client: SecretClient, secret_name: str):
    """Rotates a single secret in Key Vault.

    For secrets that mirror a live credential (the Azure SQL Server admin
    password), the real credential is updated FIRST and Key Vault is only
    written to once that succeeds -- so a failed rotation never leaves Key
    Vault holding a value that doesn't match the live system.
    """
    try:
        old_secret = client.get_secret(secret_name)
        old_version = old_secret.properties.version
    except Exception:
        old_version = "NEW"

    new_password = generate_secure_password()

    if secret_name in SQL_SERVER_ADMIN_SECRETS:
        _update_sql_server_password(new_password)
        logger.info(f"SQL Server '{SQL_SERVER_NAME}' admin login password updated live.")
        # azure-sql-jdbc-url holds a full connection string with the password
        # embedded, not a bare password -- writing generate_secure_password()
        # directly into it would corrupt the JDBC URL entirely (wrong key
        # entirely, not just a wrong value) and break every real consumer.
        new_value = _build_jdbc_url(new_password)
    else:
        new_value = new_password

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
