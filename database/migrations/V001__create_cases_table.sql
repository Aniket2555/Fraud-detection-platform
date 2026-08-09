-- ============================================================
-- V001: Primary Fraud Cases Table
-- Production fixes applied:
--   - Added CHECK constraint on case_status (prevents invalid status values)
--   - Added CHECK constraint on decision_action
--   - Added CHECK constraint on fraud_probability range [0.0, 1.0]
--   - Switched resolution_notes to NVARCHAR(MAX) for Unicode analyst notes
--   - Added correlation_id column to link back to Service Bus / Function call
--   - Added covering index on (case_status, decision_action, created_at)
--     for the most common analyst dashboard query pattern
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fraud_cases')
BEGIN
    CREATE TABLE fraud_cases (
        case_id             UNIQUEIDENTIFIER    DEFAULT NEWID()         PRIMARY KEY,
        transaction_id      VARCHAR(64)         NOT NULL                UNIQUE,
        customer_id         VARCHAR(64)         NOT NULL,
        card_id             VARCHAR(64)         NOT NULL,
        amount              DECIMAL(18, 2)      NOT NULL,
        currency            VARCHAR(3)          NOT NULL    DEFAULT 'USD',
        fraud_probability   DECIMAL(5, 4)       NOT NULL
            CONSTRAINT chk_fraud_probability   CHECK (fraud_probability BETWEEN 0.0 AND 1.0),
        scoring_mode        VARCHAR(32)         NOT NULL    DEFAULT 'full',
        decision_action     VARCHAR(32)         NOT NULL
            CONSTRAINT chk_decision_action     CHECK (decision_action IN
                ('approve', 'step_up', 'manual_review', 'block', 'approve_fallback', 'rules_only_fallback')),
        case_status         VARCHAR(32)         NOT NULL    DEFAULT 'OPEN'
            CONSTRAINT chk_case_status         CHECK (case_status IN
                ('OPEN', 'IN_REVIEW', 'ESCALATED', 'RESOLVED_FRAUD', 'RESOLVED_LEGIT', 'CLOSED')),
        assigned_analyst    VARCHAR(128)        NULL,
        model_version       VARCHAR(64)         NULL,
        shap_top_features   NVARCHAR(MAX)       NULL,       -- JSON array from SHAP explainer
        correlation_id      VARCHAR(64)         NULL,       -- Service Bus / Function correlation
        resolution          VARCHAR(32)         NULL
            CONSTRAINT chk_resolution          CHECK (resolution IS NULL OR resolution IN
                ('CONFIRMED_FRAUD', 'CONFIRMED_LEGIT', 'INCONCLUSIVE')),
        resolution_notes    NVARCHAR(MAX)       NULL,       -- Unicode analyst notes
        created_at          DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),
        updated_at          DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME()
    );

    -- Analyst dashboard: cases by status + action (covering index for top query pattern)
    CREATE INDEX IX_fraud_cases_status_action
        ON fraud_cases(case_status, decision_action, created_at DESC)
        INCLUDE (transaction_id, customer_id, fraud_probability, assigned_analyst);

    -- Customer lookup
    CREATE INDEX IX_fraud_cases_customer
        ON fraud_cases(customer_id, created_at DESC)
        INCLUDE (case_id, case_status, fraud_probability);

    -- Chronological ordering (used by data pipelines)
    CREATE INDEX IX_fraud_cases_created
        ON fraud_cases(created_at DESC);

    -- Correlation ID lookup (debugging Service Bus / Function traces)
    CREATE INDEX IX_fraud_cases_correlation
        ON fraud_cases(correlation_id)
        WHERE correlation_id IS NOT NULL;

END;
