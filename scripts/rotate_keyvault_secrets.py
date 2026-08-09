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

VAULT_NAME = os.environ.get("KEY_VAULT_NAME", "kv-fraud-dev")
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net"

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
SUBSCRIPTION_ID = os.environ.get("AZURE_SUBSCRIPTION_ID")
SQL_RESOURCE_GROUP = os.environ.get("SQL_RESOURCE_GROUP", f"rg-fraud-detection-{ENVIRONMENT}")
SQL_SERVER_NAME = os.environ.get("SQL_SERVER_NAME", f"sql-fraud-{ENVIRONMENT}")

ROTATABLE_SECRETS = [
    "db-admin-password-dev",
    "pii-hash-salt",
    "sql-admin-password-dev"
]

# These secrets mirror a live credential on the Azure SQL Server, not just a
# value read out of Key Vault. Rotating them must update the real server
# password too -- writing a fresh random value to Key Vault alone leaves the
# server's actual admin password unchanged, so every service reading the
# "rotated" secret would immediately fail to authenticate.
SQL_SERVER_ADMIN_SECRETS = {"db-admin-password-dev", "sql-admin-password-dev"}


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
    poller = sql_client.servers.begin_update(
        resource_group_name=SQL_RESOURCE_GROUP,
        server_name=SQL_SERVER_NAME,
        parameters={"administrator_login_password": new_value},
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

    new_value = generate_secure_password()

    if secret_name in SQL_SERVER_ADMIN_SECRETS:
        _update_sql_server_password(new_value)
        logger.info(f"SQL Server '{SQL_SERVER_NAME}' admin login password updated live.")

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
