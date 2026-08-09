-- ============================================================
-- V005: Step-Up Auth Log & Reference Data Tables
-- Production fix applied: added chk_auth_status CHECK constraint,
-- matching the pattern established by V001/V002 (every other
-- status/action column in this schema is CHECK-constrained).
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'step_up_requests')
BEGIN
    CREATE TABLE step_up_requests (
        step_up_id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
        transaction_id VARCHAR(64) NOT NULL,
        customer_id VARCHAR(64) NOT NULL,
        otp_code_hash VARCHAR(128) NOT NULL,
        auth_status VARCHAR(32) DEFAULT 'PENDING'
            CONSTRAINT chk_auth_status CHECK (auth_status IN
                ('PENDING', 'VERIFIED', 'FAILED', 'EXPIRED')),
        requested_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        verified_at DATETIME2 NULL,
        expired_at DATETIME2 NULL
    );
END;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'merchant_category_map')
BEGIN
    CREATE TABLE merchant_category_map (
        raw_category VARCHAR(128) PRIMARY KEY,
        standardized_category VARCHAR(128) NOT NULL,
        updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
