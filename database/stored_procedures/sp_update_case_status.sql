-- ============================================================
-- Update Case Status with Audit Trail Entry
-- Production fixes applied:
--   - event_data JSON built with STRING_ESCAPE so a quote/backslash
--     in analyst free-text @notes can't produce malformed JSON or
--     let an analyst inject extra fields into the immutable audit log
-- ============================================================
CREATE OR ALTER PROCEDURE sp_update_case_status
    @case_id UNIQUEIDENTIFIER,
    @new_status VARCHAR(32),
    @updated_by VARCHAR(128),
    @notes VARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @old_status VARCHAR(32);
    SELECT @old_status = case_status FROM fraud_cases WHERE case_id = @case_id;

    UPDATE fraud_cases
    SET case_status = @new_status, updated_at = SYSUTCDATETIME()
    WHERE case_id = @case_id;

    INSERT INTO case_events (case_id, event_type, event_data, created_by)
    VALUES (
        @case_id,
        'STATUS_CHANGED',
        CONCAT(
            '{"old_status":"', STRING_ESCAPE(@old_status, 'json'),
            '","new_status":"', STRING_ESCAPE(@new_status, 'json'),
            '","notes":"', STRING_ESCAPE(ISNULL(@notes, ''), 'json'), '"}'
        ),
        @updated_by
    );
END;
