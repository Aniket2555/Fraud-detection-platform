-- ============================================================
-- Unity Catalog Data Governance & Masking Policies
-- Restricts PII (IP, Device ID, Customer Details) for non-privileged roles.
-- Compliance Officers see unmasked data; Analysts see masked data.
--
-- Production fix applied: ALTER TABLE originally targeted
-- silver.transactions, the Phase 1 static IEEE-CIS batch table -- it has
-- no ip_address/device_id columns at all (uses device_type/device_info
-- instead). silver.streaming_transactions (the live-replay table) is the
-- one that actually carries ip_address/device_id -- same class of bug
-- already fixed in ml/training/data_preparation.py and
-- databricks/notebooks/mlops/ingest_chargeback_feedback.py this session.
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

-- 4. Masked view for non-privileged access, instead of a native column mask
-- on the raw table.
--
-- Production fix applied: ALTER TABLE ... ALTER COLUMN ... SET MASK was
-- applied for real via the SQL Warehouse (native masks require Shared-mode
-- compute; the batch-etl-dev interactive cluster is SINGLE_USER and
-- Databricks rejects policies on it outright) and verified working
-- (non-privileged query correctly returned 'ip_address'='.xxx.xxx',
-- 'device_id'='dev_****'). But it also mutated the base table's column
-- type metadata with a `STRING COLLATE UTF8_BINARY` annotation that the
-- older DBR 14.3 interactive cluster's SQL parser can't read back --
-- spark.table("...streaming_transactions") started failing with
-- [PARSE_SYNTAX_ERROR] Syntax error at or near 'COLLATE', even for a plain
-- read with no reference to the masked columns. That table is what every
-- Phase 3/4/6 pipeline script depends on (data_preparation.py,
-- retrain_pipeline.py, champion_challenger_gate.py, shadow_scoring_batch.py,
-- ingest_chargeback_feedback.py, ...), so a native mask on it isn't viable
-- while both compute engines coexist (see docs/execution-log/09-governance-
-- security.md). A view computes the mask at query time on top of the raw
-- table instead of altering the raw table's own schema at all, so nothing
-- that calls spark.table() on the base table is affected by it existing.
CREATE OR REPLACE VIEW silver.streaming_transactions_masked AS
SELECT
    * EXCEPT (ip_address, device_id),
    mask_ip_address(ip_address) AS ip_address,
    mask_device_id(device_id) AS device_id
FROM silver.streaming_transactions;

-- 5. Grant Role-Based Table Access Controls (RBAC)
-- Analysts get the masked view, not the raw table -- data-engineers and
-- platform-admins keep direct raw-table access via their schema-level
-- grants below, unaffected by this change.
GRANT SELECT ON VIEW silver.streaming_transactions_masked TO `fraud-analysts`;
GRANT SELECT ON SCHEMA gold TO `fraud-analysts`;

GRANT SELECT, MODIFY ON SCHEMA silver TO `data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA gold TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data-engineers`;

GRANT ALL PRIVILEGES ON CATALOG fraud_detection_dev TO `platform-admins`;

GRANT SELECT ON SCHEMA gold TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.reconciled_labeled_transactions TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.model_performance_kpis TO `ml-engineers`;
