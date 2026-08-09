# Phase 1 — IEEE-CIS Dataset Analysis & Schema Mapping

## Dataset Anatomy Overview

The IEEE-CIS Fraud Detection dataset consists of two main transaction tables and two identity tables joined on `TransactionID`:

- **Train Transactions:** ~590,540 rows, 394 columns (includes `isFraud` label)
- **Train Identity:** ~144,233 rows, 41 columns (~24% coverage of transactions)
- **Test Transactions:** ~506,691 rows, 393 columns (unlabeled)
- **Test Identity:** ~141,907 rows, 41 columns

---

## Primary Column Classifications

| Raw Column | Target Snake Case Column | Target DataType | Null Policy | Business Description |
|---|---|---|---|---|
| `TransactionID` | `transaction_id` | STRING | Never Null | Unique transaction identifier |
| `TransactionDT` | `transaction_dt` | INT | Never Null | Seconds from reference anchor |
| `TransactionAmt` | `transaction_amt` | DOUBLE | Never Null | Transaction monetary amount |
| `ProductCD` | `product_cd` | STRING | `"unknown"` | Product code category {W, H, C, S, R} |
| `card1`–`card6` | `card1`–`card6` | STRING | `"unknown"` | Payment card metadata (network, type, issuer) |
| `addr1`, `addr2` | `addr1`, `addr2` | DOUBLE | Preserved + `_is_null` flag | Anonymized address/billing region codes |
| `dist1`, `dist2` | `dist1`, `dist2` | DOUBLE | Preserved + `_is_null` flag | Distance metrics |
| `P_emaildomain` | `p_emaildomain` | STRING | `"unknown"` | Purchaser email domain |
| `R_emaildomain` | `r_emaildomain` | STRING | `"unknown"` | Recipient email domain |
| `C1`–`C14` | `c1`–`c14` | DOUBLE | Preserved + `_is_null` flag | Counting features associated with card/IP |
| `D1`–`D15` | `d1`–`d15` | DOUBLE | Preserved + `_is_null` flag | Timedelta features |
| `M1`–`M9` | `m1`–`m9` | INT (0/1) | Preserved | Match flags (T/F -> 1/0) |
| `V1`–`V339` | `v1`–`v339` | DOUBLE | Filtered via variance | Vesta anonymized features |
| `DeviceType` | `device_type` | STRING | `"unknown"` | Device classification (mobile/desktop) |
| `DeviceInfo` | `device_info` | STRING | `"unknown"` | User device details |

---

## Production Decisions
1. **Timestamp Anchoring:** `TransactionDT` is synthesized into `event_time` using reference date `2025-01-01T00:00:00Z`.
2. **Sentinel Nulls:** Categorical missing values are replaced with `"unknown"`. Numeric nulls are preserved while adding explicit boolean `_{col}_is_null` indicator columns.
3. **Data Splitting:** Data is chronologically split using `transaction_dt` (70% train, 15% val, 15% test) to prevent temporal data leakage.
