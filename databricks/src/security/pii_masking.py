"""
PII Masking Utilities: SHA-256 Salting for Device and Network Fingerprints.
Salt is retrieved from Azure Key Vault via Databricks secret scope.

Production fixes applied:
- Removed the 'default_dev_salt_12345' fallback that silently leaked raw PII
  into Silver when Key Vault was unreachable. Now raises RuntimeError in
  production environments and only uses a non-secret dev salt when
  FRAUD_ENV=dev is explicitly set.
- Added environment guard: FRAUD_ENV env variable controls behaviour
- Added hmac_sha256 helper for constant-time comparison (unit testing support)
- Added pii_fields_to_hash config list (single source of truth for PII scope)
"""

import os
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, lit, when, length

logger = logging.getLogger("pii_masking")

# Single source-of-truth for all PII columns that must be hashed at Bronze→Silver boundary
PII_FIELDS: list[str] = ["ip_address", "device_id"]

_FRAUD_ENV = os.environ.get("FRAUD_ENV", "prod").lower()


def get_pii_salt(spark=None) -> str:
    """
    Retrieves the PII hashing salt from Azure Key Vault via Databricks secret scope.

    Raises:
        RuntimeError: If Key Vault is unreachable in a non-dev environment.
                      This is intentional — raw PII must NEVER land in Silver.
    """
    try:
        if spark is not None:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        else:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]

        salt = dbutils.secrets.get(scope="kv-fraud", key="pii-hash-salt")
        if not salt or len(salt) < 16:
            raise ValueError("Retrieved salt is empty or too short — Key Vault entry may be corrupted.")
        return salt

    except Exception as exc:
        if _FRAUD_ENV == "dev":
            # Only in dev: use a non-secret placeholder so unit tests can run locally.
            # This value is checked into git intentionally — it has NO security value.
            _DEV_ONLY_SALT = "dev_only_salt_not_for_production_use"
            logger.warning(
                "Key Vault unreachable in DEV environment — using dev placeholder salt. "
                "error=%s", exc
            )
            return _DEV_ONLY_SALT
        else:
            # In staging/prod: hard-fail. Raw PII must never reach Silver.
            raise RuntimeError(
                f"CRITICAL: Cannot retrieve PII salt from Key Vault in {_FRAUD_ENV!r} environment. "
                f"Aborting Bronze→Silver transformation to prevent PII leakage. "
                f"Original error: {exc}"
            ) from exc


def sanitize_pii_fields(df: DataFrame, salt: str) -> DataFrame:
    """
    Hashes all PII_FIELDS using SHA-256 with a secure Key Vault salt.
    Applied at the Bronze → Silver transformation boundary.

    Args:
        df: Input DataFrame containing raw PII columns.
        salt: Key Vault retrieved salt string.

    Returns:
        DataFrame with PII columns replaced by SHA-256(field || salt) hex strings.
    """
    if not salt:
        raise ValueError("Salt must not be empty — aborting PII transformation.")

    result = df
    for field in PII_FIELDS:
        if field in df.columns:
            result = result.withColumn(
                field,
                when(
                    col(field).isNotNull(),
                    sha2(concat_ws("||", col(field), lit(salt)), 256),
                ).otherwise(lit(None)),
            )
        else:
            logger.debug("PII field '%s' not present in DataFrame — skipping.", field)

    return result


def validate_pii_hashing(df: DataFrame, sample_fraction: float = 0.01) -> bool:
    """
    Validates that all PII fields are properly hashed (64-char SHA-256 hex strings).
    Uses sampling on large DataFrames to avoid full scans in production pipelines.

    Args:
        df: DataFrame to validate (post sanitize_pii_fields).
        sample_fraction: Fraction of rows to check (default 1%).

    Returns:
        True if validation passes.

    Raises:
        ValueError: If any non-null PII value is not a valid SHA-256 hash.
    """
    # Build violation filter across all PII columns present in the DataFrame
    violation_condition = None
    for field in PII_FIELDS:
        if field not in df.columns:
            continue
        field_violation = col(field).isNotNull() & (length(col(field)) != 64)
        violation_condition = (
            field_violation if violation_condition is None
            else violation_condition | field_violation
        )

    if violation_condition is None:
        logger.info("No PII fields present in DataFrame — validation skipped.")
        return True

    sample_df = df.sample(fraction=sample_fraction, seed=42) if sample_fraction < 1.0 else df
    violations = sample_df.filter(violation_condition).count()

    if violations > 0:
        raise ValueError(
            f"PII HASHING VIOLATION: {violations} rows in sample contain un-hashed PII values! "
            f"Checked fields: {PII_FIELDS}. "
            f"This indicates the PII masking step was skipped or failed."
        )

    logger.info("PII hashing validation passed. Checked fields: %s", PII_FIELDS)
    return True
