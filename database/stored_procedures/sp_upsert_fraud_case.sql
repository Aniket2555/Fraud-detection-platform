-- ============================================================
-- Idempotent Stored Procedure: Upsert Fraud Case (Handles Retries)
-- Also inserts a CASE_CREATED or MODEL_SCORE_UPDATED event into the
-- case_events audit trail, matching whether this call inserted or
-- updated the case.
-- Production fixes applied:
--   - event_type corrected to 'CASE_CREATED' (was 'CREATED', which
--     violated chk_case_event_type on every single call)
--   - MERGE now uses HOLDLOCK to prevent the WHEN NOT MATCHED race
--     (two concurrent inserts for the same new transaction_id could
--     both pass the match check under READ COMMITTED)
--   - WHEN MATCHED now refreshes all mutable fields, not just
--     fraud_probability, so a re-score with a different decision
--     doesn't leave stale decision_action/scoring_mode/model_version
--   - $action drives the audit event type so retries of an existing
--     case log MODEL_SCORE_UPDATED instead of a duplicate CASE_CREATED
--   - event_data JSON built with STRING_ESCAPE to prevent malformed/
--     injected JSON from free-text-derived values
--   - now returns case_id as a result set (was computed into a local
--     variable and never actually returned to the caller) -- callers
--     like logic-apps/workflows/workflow_stepup_auth.json need it for
--     the subsequent sp_update_case_status call and had no way to get it
-- ============================================================
CREATE OR ALTER PROCEDURE sp_upsert_fraud_case
    @transaction_id VARCHAR(64),
    @customer_id VARCHAR(64),
    @card_id VARCHAR(64),
    @amount DECIMAL(18, 2),
    @currency VARCHAR(3),
    @fraud_probability DECIMAL(5, 4),
    @scoring_mode VARCHAR(32),
    @decision_action VARCHAR(32),
    @model_version VARCHAR(32),
    @shap_top_features NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @case_id UNIQUEIDENTIFIER;
    DECLARE @merge_action VARCHAR(10);
    DECLARE @merge_output TABLE (case_id UNIQUEIDENTIFIER, action VARCHAR(10));

    MERGE fraud_cases WITH (HOLDLOCK) AS target
    USING (SELECT @transaction_id AS transaction_id) AS source
    ON (target.transaction_id = source.transaction_id)

    WHEN MATCHED THEN
        UPDATE SET
            target.fraud_probability = @fraud_probability,
            target.scoring_mode = @scoring_mode,
            target.decision_action = @decision_action,
            target.model_version = @model_version,
            target.shap_top_features = @shap_top_features,
            target.updated_at = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN
        INSERT (
            transaction_id, customer_id, card_id, amount, currency,
            fraud_probability, scoring_mode, decision_action, case_status,
            model_version, shap_top_features
        )
        VALUES (
            @transaction_id, @customer_id, @card_id, @amount, @currency,
            @fraud_probability, @scoring_mode, @decision_action, 'OPEN',
            @model_version, @shap_top_features
        )

    OUTPUT inserted.case_id, $action INTO @merge_output;

    SELECT TOP 1 @case_id = case_id, @merge_action = action FROM @merge_output;

    IF @case_id IS NOT NULL
    BEGIN
        INSERT INTO case_events (case_id, event_type, event_data, created_by)
        VALUES (
            @case_id,
            CASE WHEN @merge_action = 'INSERT' THEN 'CASE_CREATED' ELSE 'MODEL_SCORE_UPDATED' END,
            CONCAT(
                '{"decision_action":"', STRING_ESCAPE(@decision_action, 'json'),
                '","fraud_probability":', CAST(@fraud_probability AS VARCHAR),
                ',"scoring_mode":"', STRING_ESCAPE(@scoring_mode, 'json'), '"}'
            ),
            'SYSTEM'
        );
    END;

    SELECT @case_id AS case_id;
END;
