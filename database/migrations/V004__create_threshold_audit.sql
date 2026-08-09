-- ============================================================
-- V004: Threshold Audit Trail (Compliance Requirement)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'threshold_audit')
BEGIN
    CREATE TABLE threshold_audit (
        change_id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
        parameter_name VARCHAR(128) NOT NULL,
        old_value VARCHAR(64) NOT NULL,
        new_value VARCHAR(64) NOT NULL,
        changed_by VARCHAR(128) NOT NULL,
        changed_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        change_reason VARCHAR(500) NULL
    );
END;
