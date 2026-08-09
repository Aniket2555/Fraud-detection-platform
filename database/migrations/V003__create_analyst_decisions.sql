-- ============================================================
-- V003: Analyst Decisions & Review History
-- Production fix applied: added chk_decision_result CHECK constraint,
-- matching the enum V001's chk_resolution already established
-- (fraud_cases.resolution) and the values
-- databricks/notebooks/mlops/ingest_chargeback_feedback.py actually
-- filters on ('CONFIRMED_FRAUD' / 'CONFIRMED_LEGIT'). Without this, every
-- other status/action/resolution column in this schema (V001, V002) is
-- CHECK-constrained except this one, so a typo'd value here would
-- silently fail to match those filters instead of erroring.
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'analyst_decisions')
BEGIN
    CREATE TABLE analyst_decisions (
        decision_id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
        case_id UNIQUEIDENTIFIER NOT NULL FOREIGN KEY REFERENCES fraud_cases(case_id),
        analyst_id VARCHAR(128) NOT NULL,
        decision_result VARCHAR(32) NOT NULL
            CONSTRAINT chk_decision_result CHECK (decision_result IN
                ('CONFIRMED_FRAUD', 'CONFIRMED_LEGIT', 'INCONCLUSIVE')),
        decision_notes VARCHAR(1000) NULL,
        decided_at DATETIME2 DEFAULT SYSUTCDATETIME()
    );
END;
