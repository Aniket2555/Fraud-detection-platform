# Feature Store Catalog

This catalog documents all 43 features across 6 feature families implemented in Phase 3.

---

## 1. Stateless Transaction Features (`FF_TRX`) — 11 Features
- `feature_log_amount`: `log1p(amount)`
- `feature_hour_of_day`: Hour of transaction [0-23]
- `feature_day_of_week`: Day of week [1-7]
- `feature_is_weekend`: Flag indicating weekend transaction (1/0)
- `feature_is_night`: Flag indicating late-night transaction 00:00–05:59 (1/0)
- `feature_billing_shipping_mismatch`: Flag indicating cross-border shipping mismatch
- `feature_amount_bucket`: Categorical tier {micro, small, medium, large, high_value}
- `feature_card_merchant_pair`: SHA256 entity hash signature
- `feature_payment_method_idx`: Integer-encoded payment instrument
- `feature_channel_idx`: Integer-encoded origin channel
- `feature_amount_zscore_vs_baseline`: Deviation from customer 90-day mean

---

## 2. Card & Customer Velocity (`FF_VEL`) — 12 Features
- `vel_card_txn_count_5m`: 5-minute sliding card transaction count
- `vel_card_avg_amount_5m`: 5-minute sliding average amount
- `vel_card_std_amount_5m`: 5-minute sliding standard deviation
- `vel_card_max_amount_5m`: 5-minute sliding max amount
- `vel_card_sum_amount_5m`: 5-minute sliding total amount
- `vel_card_txn_count_1h`: 1-hour sliding card transaction count
- `vel_card_sum_amount_1h`: 1-hour sliding card total amount
- `vel_cust_txn_count_1h`: 1-hour sliding customer transaction count
- `vel_cust_avg_amount_1h`: 1-hour sliding customer average amount
- `vel_cust_sum_amount_1h`: 1-hour sliding customer total amount
- `vel_cust_distinct_merchants_1h`: 1-hour distinct merchant count
- `vel_cust_distinct_devices_24h`: 24-hour distinct device count

---

## 3. Geo-Velocity & Impossible Travel (`FF_GEO`) — 4 Features
- `geo_dist_km`: Great-circle Haversine distance between consecutive card transactions
- `time_delta_hours`: Time difference in hours
- `geo_implied_speed_kmh`: Calculated travel speed (km/h)
- `geo_flag_impossible_travel`: Flag set to 1 if speed > 900 km/h

---

## 4. Merchant Risk Features (`FF_MERCH`) — 4 Features
- `merch_avg_ticket_30d`: 30-day average transaction amount
- `merch_txn_count_30d`: 30-day transaction volume
- `merch_fraud_count_30d`: 30-day historical fraud count
- `merch_fraud_ratio_30d`: 30-day fraud ratio

---

## 5. Behavioral & Customer Baseline (`FF_BASE`) — 7 Features
- `base_cust_90d_avg_amount`: 90-day average amount
- `base_cust_90d_std_amount`: 90-day standard deviation
- `base_cust_90d_txn_count`: 90-day transaction count
- `base_cust_90d_median_amount`: 90-day median transaction amount
- `base_cust_tenure_days`: Account tenure in days
- `base_cust_first_seen_ts`: First transaction timestamp
- `base_cust_last_seen_ts`: Most recent transaction timestamp

---

## 6. Graph Features (`FF_GRAPH`) — 5 Features
- `graph_device_sharing_count_24h`: Distinct customers on device in 24 hours
- `graph_ip_sharing_count_1h`: Distinct cards on IP hash in 1 hour
- `graph_pagerank_score`: Entity PageRank centrality score
- `graph_degree_centrality`: Total connected edges count
- `graph_community_id`: Connected component partition ID
