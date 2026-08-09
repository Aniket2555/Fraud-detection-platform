-- ============================================================
-- Unity Catalog Data Governance & Masking Policies
-- Restricts PII (IP, Device ID, Customer Details) for non-privileged roles.
-- Compliance Officers see unmasked data; Analysts see masked data.
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

-- 4. Apply Column Masking Policies on Silver Transactions Table
ALTER TABLE silver.transactions ALTER COLUMN ip_address SET MASK mask_ip_address;
ALTER TABLE silver.transactions ALTER COLUMN device_id SET MASK mask_device_id;

-- 5. Grant Role-Based Table Access Controls (RBAC)
GRANT SELECT ON TABLE silver.transactions TO `fraud-analysts`;
GRANT SELECT ON SCHEMA gold TO `fraud-analysts`;

GRANT SELECT, MODIFY ON SCHEMA silver TO `data-engineers`;
GRANT SELECT, MODIFY ON SCHEMA gold TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO `data-engineers`;

GRANT ALL PRIVILEGES ON CATALOG fraud_detection_dev TO `platform-admins`;

GRANT SELECT ON SCHEMA gold TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.reconciled_labeled_transactions TO `ml-engineers`;
GRANT SELECT, MODIFY ON TABLE gold.model_performance_kpis TO `ml-engineers`;
