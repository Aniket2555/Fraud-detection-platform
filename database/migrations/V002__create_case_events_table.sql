-- ============================================================
-- V002: Case Events Audit Trail (Immutable)
-- Production fixes applied:
--   - Changed event_data from NVARCHAR(MAX) NULL to NVARCHAR(MAX) NOT NULL DEFAULT '{}'
--     Prevents NULL event payloads which break JSON parsing in analytics queries
--   - Added event_type CHECK constraint (validates against known event taxonomy)
--   - Added correlation_id column to trace back to Function invocations
--   - Added IX_case_events_type index for event-type aggregate analytics
--   - Table is intentionally INSERT-only (no UPDATE/DELETE) — enforced by app logic
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'case_events')
BEGIN
    CREATE TABLE case_events (
        event_id        UNIQUEIDENTIFIER    DEFAULT NEWID()     PRIMARY KEY,
        case_id         UNIQUEIDENTIFIER    NOT NULL
            CONSTRAINT fk_case_events_case  FOREIGN KEY REFERENCES fraud_cases(case_id)
            ON DELETE CASCADE,
        event_type      VARCHAR(64)         NOT NULL
            CONSTRAINT chk_case_event_type  CHECK (event_type IN (
                'CASE_CREATED', 'STATUS_CHANGED', 'ANALYST_ASSIGNED',
                'STEP_UP_REQUESTED', 'STEP_UP_VERIFIED', 'STEP_UP_FAILED',
                'RESOLUTION_SET', 'ESCALATED', 'CLOSED', 'NOTE_ADDED',
                'MODEL_SCORE_UPDATED', 'MANUAL_REVIEW_STARTED'
            )),
        event_data      NVARCHAR(MAX)       NOT NULL    DEFAULT '{}',   -- JSON payload, never NULL
        correlation_id  VARCHAR(64)         NULL,
        created_at      DATETIME2           NOT NULL    DEFAULT SYSUTCDATETIME(),
        created_by      VARCHAR(128)        NOT NULL    DEFAULT 'SYSTEM'
    );

    -- Primary lookup: all events for a case in chronological order
    CREATE INDEX IX_case_events_case
        ON case_events(case_id, created_at ASC);

    -- Event-type analytics (e.g., count of STEP_UP_FAILED per day)
    CREATE INDEX IX_case_events_type
        ON case_events(event_type, created_at DESC);

END;
