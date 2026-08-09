# Case Management & Operational Workflow Guide

This document specifies the operational decision workflow, Azure SQL schema architecture, and decision API contracts for Phase 5.

---

## 1. Score Band Actions

| Risk Score Band | Action | Processor | SLA | Action Summary |
|---|---|---|---|---|
| `[0.00, 0.10)` | `approve` | Fast-Path Azure Function | <15ms | Synchronously approve transaction |
| `[0.10, 0.60)` | `step_up` | Service Bus + Logic Apps | 5 min | Trigger OTP challenge; wait 5m before escalation |
| `[0.60, 0.90]` | `manual_review` | Service Bus + Azure SQL | 30 min | Queue in analyst workbench for human audit |
| `(0.90, 1.00]` | `block` | Fast-Path Azure Function | <15ms | Synchronously block transaction & freeze card |

---

## 2. Azure SQL Case Database Schema

The database consists of 5 core tables:
1. `fraud_cases`: System of record for generated fraud cases (stores scoring mode, model version, SHAP JSON).
2. `case_events`: Immutable audit trail tracking state changes (`CREATED`, `STATUS_CHANGED`, `ESCALATED`).
3. `analyst_decisions`: History of manual analyst actions and audit notes.
4. `threshold_audit`: Compliance audit trail for all threshold changes.
5. `step_up_requests`: Log of OTP step-up authentication attempts.

---

## 3. Decision API Contract

### Request: `POST /api/evaluate-decision`
```json
{
    "transaction_id": "txn_1001",
    "customer_id": "cust_101",
    "card_id": "card_202",
    "amount": 450.00,
    "currency": "USD",
    "fraud_probability": 0.72,
    "scoring_mode": "full",
    "model_version": "v1.0.0",
    "top_risk_factors": [
        {"feature": "vel_card_txn_count_5m", "shap_value": 0.34, "direction": "increases_risk"}
    ]
}
```

### Response: Status 202 Accepted
```json
{
    "transaction_id": "txn_1001",
    "decision_action": "manual_review",
    "fraud_probability": 0.72,
    "status": "PENDING_ASYNC",
    "latency_ms": 8.42
}
```
