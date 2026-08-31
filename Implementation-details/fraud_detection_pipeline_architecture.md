# Real-Time Fraud Detection Platform — Azure Data Engineering Architecture

**Scope:** Production-grade lakehouse + streaming + feature store architecture for a hybrid fraud detection system, built entirely on Azure.

**Data sources:** IEEE-CIS Fraud Detection (historical training), Kaggle Credit Card Transactions (streaming simulation), synthetic fraud generator, hybrid anomaly detection (supervised + unsupervised + graph).

---

## 1. Design Principles & SLAs

| Requirement | Target |
|---|---|
| End-to-end inference latency (event → decision) | p99 < 100 ms |
| Feature freshness (online store) | < 5 s lag from event time |
| Throughput | 5,000–20,000 events/sec sustained, burst to 50k |
| Training/serving skew | Zero — same feature logic for offline and online |
| Data durability | Bronze retained indefinitely (append-only, replayable) |
| Availability | 99.95% for the scoring path |
| Compliance | PCI-DSS scope isolation, PII masking, full lineage/audit trail |

Two architectural decisions drive everything below:

1. **One feature computation logic, two materialization targets.** Features are defined once (as Spark transformations) and materialized to both an **offline store** (for training) and an **online store** (for inference), via Azure ML's managed feature store. This is the single most important design choice for a fraud system — it's what prevents the #1 cause of production fraud-model failure: training/serving skew.
2. **Bronze is immutable and replayable.** Every model bug, every "why did we approve/block this transaction" audit question, and every backfill is answered by replaying Bronze — never by re-querying a live source.

A note on platform choice: this uses **Azure Machine Learning** (workspace, managed feature store, managed online endpoints, MLflow-native tracking) rather than **Azure AI Foundry**. Foundry is Microsoft's platform for LLM/agentic/GenAI workloads; this is a classical tabular MLOps problem (train on your own data, score against a business outcome, operationalize with lineage and drift monitoring) — that's squarely Azure ML's use case.

---

## 2. High-Level Architecture

```
┌─────────────────────────── DATA SOURCES ───────────────────────────┐
│  IEEE-CIS (historical, batch)   Credit-Card txns (replayed stream)  │
│  Synthetic fraud generator (injected into stream)                   │
└───────────────────────────────────────────────────────────────────┘
        │ batch (ADF)                    │ streaming (producer → Event Hubs)
        ▼                                 ▼
┌─────────────────┐             ┌──────────────────────┐
│ ADLS Gen2 (raw   │             │  Azure Event Hubs      │
│ landing, IEEE-CIS│             │  (Kafka-compatible,    │
│ CSVs)            │             │  partition key =       │
└─────────────────┘             │  card_id / customer_id)│
        │                        └──────────┬────────────┘
        │                                    │
        └───────────────┬────────────────────┘
                         ▼
         ┌───────────────────────────────┐
         │ Azure Databricks — Structured  │
         │ Streaming + batch jobs         │
         │ (Autoloader, schema evolution) │
         └───────────────┬────────────────┘
                          │
         ┌────────────────┴────────────────┐
         ▼                                  ▼
 ┌─────────────────┐              ┌──────────────────────┐
 │ BRONZE (Delta)   │              │ Dead-letter / quarantine│
 │ append-only, raw │              │ (ADLS + Event Hubs DLQ) │
 └────────┬─────────┘              └──────────────────────┘
          ▼
 Data-quality gate (PyDeequ / Great Expectations, Unity Catalog constraints)
          ▼
 ┌─────────────────┐
 │ SILVER (Delta)   │  cleaned, deduped, PII-masked, conformed
 └────────┬─────────┘
          ▼
 ┌─────────────────────────────┐
 │ Streaming feature engineering │  (windowed aggregates, velocity,
 │ (Spark Structured Streaming,  │   geo-velocity, graph features)
 │ stateful, watermarked)        │
 └───────┬───────────────┬──────┘
         ▼               ▼
 ┌───────────────┐  ┌───────────────────────┐
 │ Offline store  │  │ Online store            │
 │ (ADLS Gen2 /   │  │ (Azure Managed Redis,   │
 │  Delta)        │  │  <10ms reads)           │
 └───────┬────────┘  └───────────┬─────────────┘
         ▼                       ▼
 ┌──────────────────┐   ┌─────────────────────────────┐
 │ Training pipeline  │   │ Real-time inference           │
 │ (Azure ML pipelines,│  │ Azure ML Managed Online       │
 │  MLflow, registry)  │  │ Endpoint — hybrid ensemble    │
 └──────────┬──────────┘   │ (XGBoost/LightGBM + AE + IF) │
            │               └─────────────┬─────────────┘
            │                             ▼
            │                   ┌───────────────────┐
            │                   │ Decision engine     │
            │                   │ (Azure Function)    │
            │                   │ fast path: approve /  │
            │                   │ block (score bands)    │
            │                   └─────────┬─────────────┘
            │                             ▼
            │                   Service Bus Topic (fraud-case-events)
            │                     │            │             │
            │                     ▼            ▼             ▼
            │               Logic App    Azure SQL      Compliance
            │               (step-up /   (case mgmt      audit log
            │                manual-      system-of-
            │                review       record)
            │                workflow,
            │                OTP wait,
            │                escalation)
            ▼
 ┌────────────────────┐
 │ GOLD (Delta / Synapse)│  daily fraud KPIs, risk scores, model metrics
 └───────────┬─────────┘
             ▼
     Power BI · Compliance reporting · Drift-triggered retraining
```

---

## 3. Data Sources → Ingestion Mapping

Each of your three data sources plays a distinct, deliberate role — they are not interchangeable:

| Source | Role | How it enters the pipeline |
|---|---|---|
| **IEEE-CIS Fraud Detection** | Historical ground truth for supervised model training; validation/backtesting set | Batch-loaded once via **Azure Data Factory** from source CSVs into ADLS Gen2 `raw/ieee-cis/`, then processed through the same Bronze→Silver→Gold path as streaming data (so training features are computed with *identical* logic to production features) |
| **Kaggle Credit Card Transactions** | Simulates a live production transaction feed, to prove out the streaming path end-to-end (ingestion → features → online store → inference → decision) | A Python **producer service** replays rows chronologically (respecting relative inter-arrival times, or accelerated) into **Azure Event Hubs**, mimicking a real payment gateway |
| **Synthetic fraud generator** | (a) augments the minority (fraud) class for training, since real fraud is ~0.1–0.5% of transactions, and (b) continuously injects novel fraud patterns into the live stream for adversarial testing / drift stress-testing | A generation service (SMOTE/ADASYN for tabular augmentation, or a CTGAN trained on the fraud subclass for more realistic synthetic rows) writes to two places: batch Parquet files for offline training augmentation, and an injection stream merged into the Event Hub for live testing |

### 3.1 Event schema (canonical, applies to both real and synthetic events)

```json
{
  "transaction_id": "uuid",
  "schema_version": "1.0",
  "event_time": "2026-07-24T10:21:45.123Z",
  "ingestion_time": "2026-07-24T10:21:45.410Z",
  "customer_id": "c_512",
  "card_id": "card_9931",
  "amount": 2500.00,
  "currency": "INR",
  "merchant_id": "m_4471",
  "merchant_category": "ecommerce",
  "payment_method": "credit_card",
  "device_id": "d_abc123",
  "ip_address": "132.24.x.x",
  "latitude": 18.54,
  "longitude": 73.82,
  "billing_country": "IN",
  "shipping_country": "IN",
  "channel": "mobile_app",
  "is_recurring": false,
  "session_id": "sess_xyz789",
  "is_synthetic": false,
  "source": "credit_card_txn_stream"
}
```

**Schema field additions (rationale):**
- `schema_version` — enables safe application-level branching as the schema evolves; the Schema Registry enforces format, but version tagging enables consumers to handle fields conditionally without breaking.
- `billing_country` / `shipping_country` — cross-border mismatch is one of the strongest e-commerce fraud signals; deriving this from lat/long is lossy and adds latency, so pass it from the gateway directly.
- `is_recurring` — recurring payments have fundamentally different fraud patterns (low risk for same-merchant repeats, high risk if payment details suddenly change); treating them identically hurts model precision.
- `session_id` — links transactions to broader browsing/authentication sessions, enabling session-level anomaly features (e.g., multiple cards tried in one session = card-testing pattern).

`is_synthetic` and `source` are important — they let Bronze/Silver keep real and synthetic events physically together (so streaming feature logic is exercised identically) while Gold and model-training joins can filter/label by origin.

### 3.2 Azure Event Hubs configuration

- **Partition key = `card_id`** (not `customer_id` alone) — this keeps velocity/sequence features correct per instrument, which is what most card-fraud patterns hinge on (card testing, BIN attacks).
- **Capture enabled** → Event Hubs Capture automatically archives raw events to ADLS Gen2 in Avro, giving you a second, zero-code path to Bronze as a safety net independent of the Databricks job.
- **Throughput units / Processing Units** sized for burst headroom (auto-inflate enabled); use the **dedicated tier** or Premium tier once volume exceeds standard tier limits, to isolate the fraud workload from noisy-neighbor throttling.
- **Schema Registry** (Event Hubs' built-in schema registry, Avro) enforces the canonical schema at write time, rejecting malformed producers before they ever reach Spark — cheaper than catching it in the DLQ.
- Producers other than the simulator (POS/ATM/mobile/UPI in a real deployment) publish to the same Event Hub namespace via consumer groups, so this scales to true multi-channel ingestion without redesign.

---

## 4. Medallion Lakehouse (Azure Databricks + Delta Lake on ADLS Gen2)

### 4.1 Bronze — raw, append-only, replayable

- Consumed via **Databricks Structured Streaming with Auto Loader** (`cloudFiles` for the ADF-landed IEEE-CIS files; native Event Hubs connector for the live stream).
- **Schema evolution mode:** `addNewColumns` — new fields from upstream producers don't break the pipeline; they land in Bronze and are explicitly handled (or ignored) in Silver.
- No transformation beyond: parse, attach `_ingested_at`, attach `_source_partition`, checkpoint. Nothing is dropped, corrected, or deduplicated here.
- **Partitioning:** `event_date` (derived from `event_time`) — balances write throughput with downstream query pruning.
- **Checkpointing** to a dedicated ADLS Gen2 checkpoint path per stream, so job restarts resume exactly-once (Delta's transaction log + Structured Streaming checkpoints together give end-to-end exactly-once semantics from Event Hubs to Bronze).

```python
# Bronze ingestion (Databricks Structured Streaming, Event Hubs source)
from pyspark.sql.functions import current_timestamp, col, from_json

eh_conf = {
    "eventhubs.connectionString": dbutils.secrets.get("kv-fraud", "eventhub-conn-str"),
    "eventhubs.consumerGroup": "bronze-ingest"
}

raw_stream = (
    spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
)

parsed = (
    raw_stream
        .withColumn("body", col("body").cast("string"))
        .withColumn("event", from_json(col("body"), txn_schema))
        .select("event.*", "enqueuedTime", "partition")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("event_date", col("event_time").cast("date"))  # required for partitionBy
)

(parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_txn")
    .option("maxEventsPerTrigger", 100000)  # cap per-batch volume during lag catch-up
    .partitionBy("event_date")
    .trigger(processingTime="5 seconds")
    .toTable("bronze.transactions"))
```

**Bronze-level checks only:** JSON/Avro parse failures, schema-registry rejects, and duplicate `transaction_id` within the micro-batch route to a **Dead Letter Queue** (a second Event Hub / ADLS `quarantine/` path) with the failure reason attached — never silently dropped.

#### Back-pressure & consumer lag management

- **Event Hubs retention:** Set to **7 days** (maximum for Premium tier) rather than the default 1 day — provides recovery headroom when the streaming job is down for maintenance or incident recovery.
- **Consumer lag alerting:** Monitor `enqueuedTime - processingTime` per partition via Databricks metrics → Azure Monitor; alert when lag exceeds **2× the watermark (20 min)** — this signals degraded feature quality *before* windows start closing with incomplete data.
- **`maxEventsPerTrigger`** (shown in the code above) caps per-batch volume during catch-up after an outage, preventing OOM on the Spark executors when recovering a large backlog.
- **Event Hubs Capture as recovery path:** During extended Databricks outages, Capture's Avro files in ADLS become the explicit recovery route via Auto Loader backfill — not just an archive. Document the runbook step that switches the job to consume from Capture rather than the live stream for recovery.

#### Databricks runtime & cluster configuration

- Use **Databricks Runtime 14.x LTS** (or later LTS release) for 24/7 streaming jobs — LTS gives a 2-year support window with no forced upgrades during a run.
- Streaming jobs run on a **dedicated job cluster** (not a shared interactive cluster) with autoscaling between min/max nodes sized for the 50k events/sec burst target.
- Enable **Photon** for Silver/Gold batch transformation jobs — significant speedup on Delta `MERGE` operations.
- Use **Unity Catalog** as the metastore (not the legacy Hive metastore) for consistent lineage and governance across Bronze/Silver/Gold.

### 4.2 Silver — cleaned, conformed, trustworthy

Silver is where correctness lives. Operations, all idempotent (MERGE-based upserts on `transaction_id`):

- **Deduplication:** `MERGE INTO silver.transactions USING bronze_batch ON transaction_id WHEN NOT MATCHED THEN INSERT` — handles Event Hubs' at-least-once delivery.
- **Null/missing handling:** categorical nulls → `"unknown"` sentinel (never silently imputed to a misleading default like `"other"`, which pollutes categorical encodings).
- **Timestamp normalization** to UTC; local-time-derived features (hour-of-day, is_weekend) computed relative to the *customer's* registered timezone, not UTC, since fraud behavioral signals are local-time-sensitive.
- **Categorical standardization** via a maintained lookup table (`ref.merchant_category_map`), rather than ad hoc string matching — auditable and versioned. The table itself lives in **Azure SQL Database**, where fraud-ops/compliance actually edit reference data (merchant categories, FX rates, risk-tier overrides), and is synced into Silver via a scheduled Data Factory copy — Azure SQL is the system of record for anything a human edits by hand; Delta stays the analytical copy.
- **Geo validation:** `latitude ∈ [-90,90]`, `longitude ∈ [-180,180]`; invalid coordinates are nulled (not row-dropped) and flagged `geo_invalid = true` so downstream distance features degrade gracefully instead of crashing.
- **Currency normalization** to a base currency (USD) using a daily FX-rate reference table joined in, for amount-based features to be comparable across `currency`.
- **PII handling (PCI-DSS relevant):**
  - PAN/card numbers are **never** stored in Bronze/Silver in full — only a tokenized `card_id` (tokenization happens upstream at the gateway/producer, not in the lakehouse).
  - IP addresses and device IDs are hashed (salted) before Silver for privacy while remaining joinable.
  - **Microsoft Purview** scans Silver/Gold for PII classification, enforced by Unity Catalog column-level ACLs and dynamic views (analysts see masked columns unless explicitly granted).

Data quality gate between Bronze and Silver — **PyDeequ** (or Great Expectations) constraint suite, run as a Databricks job step, blocking promotion on failure of hard constraints:

```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

check = (Check(spark, CheckLevel.Error, "txn_silver_gate")
    .isComplete("transaction_id")
    .isUnique("transaction_id")
    .isNonNegative("amount")
    .isContainedIn("currency", currency_whitelist)
    .satisfies("latitude BETWEEN -90 AND 90 OR latitude IS NULL", "lat_range")
)

result = VerificationSuite(spark).onData(silver_batch_df).addCheck(check).run()
```

Failed rows → `quarantine/silver_rejects/` with the failing constraint attached, surfaced on a data-quality dashboard, not just logged.

### 4.3 Gold — business-ready, aggregated

- `gold.daily_fraud_summary`, `gold.merchant_risk_scores`, `gold.customer_risk_profile`, `gold.hourly_txn_stats`, `gold.model_performance_daily`.
- Served to **Azure Synapse serverless SQL pool** (or Databricks SQL warehouse) for BI query performance, then **Power BI** for dashboards (fraud ops, compliance, executive).
- Gold also feeds **model retraining triggers**: e.g., `gold.model_performance_daily` is what the drift/monitoring job reads to decide whether to kick off retraining (Section 8).

---

## 5. Streaming Feature Engineering

This is the highest-leverage part of the system — a fraud model is only as good as its features, and in fraud specifically, most signal lives in *behavior relative to history*, not the raw transaction.

Implemented as **Spark Structured Streaming with stateful aggregations**, using `flatMapGroupsWithState` (or windowed aggregations with watermarking) keyed by `card_id` / `customer_id` / `merchant_id`, so state (running sums, counts, last-seen location) persists across micro-batches without recomputing from full history each time.

```python
from pyspark.sql.functions import window, avg, stddev, count, max as smax

velocity_features = (
    silver_stream
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            col("card_id"),
            window(col("event_time"), "5 minutes", "1 minute")  # sliding window
        )
        .agg(
            count("transaction_id").alias("txn_count_5m"),
            avg("amount").alias("avg_amount_5m"),
            stddev("amount").alias("std_amount_5m"),
            smax("amount").alias("max_amount_5m"),
        )
)
```

**Watermarking (10 min)** bounds how long the job waits for late-arriving events before finalizing a window — necessary for bounded state size in a 24/7 job, and set based on measured producer/network jitter, not guessed.

#### Watermark trade-off decision

- **10 minutes** is the starting value based on an assumed upper bound of producer/network jitter. Measure actual late-arrival distribution during Phase 2 (streaming path validation) and adjust — too tight drops legitimate late events from windows; too loose increases state size and delays window output.
- For **geo-velocity features** specifically (impossible travel detection), use a separate stream fork with a **30-minute watermark** — these features tolerate higher output latency but are highly sensitive to dropped intermediate events (a missing location hop makes a legitimate journey look impossible).
- Emit a **`late_events_dropped_count`** metric per partition per minute to Azure Monitor — a spike here directly indicates degraded feature quality and should trigger investigation before it shows up as model drift.

### 5.1 Feature families

| Family | Examples | Computation |
|---|---|---|
| **Transaction-level** | `log_amount`, `hour_of_day`, `is_weekend`, `is_holiday`, `payment_method_onehot` | Stateless, per-event |
| **Velocity (card/customer)** | txns per 5min/1h/24h, sum/avg/std amount per window, time-since-last-txn | Stateful windowed aggregation, sliding 1–5 min windows over 5min/1h/24h |
| **Merchant** | merchant historical fraud ratio, merchant avg ticket size, new-merchant-for-this-customer flag | Joined from a slowly-changing `gold.merchant_risk_scores` dimension, refreshed hourly |
| **Geo / device velocity** | distance from last transaction, implied travel speed (km/h), country/city mismatch, "impossible travel" flag | Haversine distance between consecutive event locations per `card_id`, using stateful last-known-location |
| **Device / channel** | new-device flag, device age, IP reputation, VPN/proxy flag | Joined from device-fingerprint reference table + 3rd-party IP reputation feed (Azure API Management-fronted external API) |
| **Behavioral** | first international purchase, night-time purchase flag, amount z-score vs. customer's 90-day baseline | Requires customer baseline stats, computed in the **offline batch feature pipeline** and served from the feature store (not recomputed per-event) |
| **Graph / relational** | shared-device count across customers, shared-IP count across cards, entity centrality in the customer–merchant–device graph | Two-tier approach (see §5.3 below) |

### 5.2 Graph feature architecture (two-tier)

Full graph computation at streaming speed is impractical — community detection and centrality scoring on a production-scale entity graph takes 10–20 minutes even on a large cluster. A two-tier approach solves this:

1. **Pre-computed graph metrics** (degree centrality, PageRank, community assignment per entity) — computed via **GraphFrames on a dedicated Databricks job cluster** every **1 hour**, then materialized to the offline/online feature store as slowly-changing features. This is what the model receives at inference time for graph signals like entity centrality and community fraud rate.
2. **Real-time local graph queries** — for fresh, low-hop features like "how many distinct customers have used this device in the last 24h," use **Azure Cosmos DB (Gremlin API)** with a 1-hop bounded traversal at inference time (bounded query cost, sub-10ms). The entity graph in Cosmos is updated by a lightweight streaming Databricks job that writes/updates edges as transactions clear Silver.

This avoids the trap of trying to do full graph recomputation at streaming speed while still providing fresh relational signals on the critical scoring path.

### 5.3 Offline/online consistency

The **same PySpark transformation code** defines each feature (as an Azure ML feature-set specification, Section 6) — it's not "similar logic re-implemented in Python for training and Java/Scala for serving." One transformation, two materialization sinks. This is the concrete mechanism that prevents training/serving skew, not just a design aspiration.

---

## 6. Feature Store — Azure ML Managed Feature Store

Azure ML's managed feature store (GA, integrates with both Databricks and Azure ML native Spark) is the right fit here over rolling a bespoke Feast+Redis stack, because it natively gives you:

- **One feature-set spec → both stores.** Offline materialization to ADLS Gen2/Delta, online materialization to a Redis-backed cache, from a single definition — no separate online/offline pipelines to keep in sync.
- **Point-in-time correct joins** for training-set generation (critical for fraud: you must join features *as they were known at transaction time*, not with future information leaking in — a common and dangerous bug in fraud model training).
- **Managed Spark serverless compute** runs materialization jobs — no cluster ops for this piece.
- **Declarative feature retrieval component** usable directly inside Azure ML training pipelines and batch-inference pipelines.

> **Note:** Azure Cache for Redis is on a retirement path in favor of **Azure Managed Redis** — provision the online materialization store on Azure Managed Redis from day one rather than the legacy SKU.

### 6.1 Feature set specification (example)

```python
from azureml.featurestore import FeatureStoreClient
from azure.ai.ml.entities import FeatureSet, FeatureSetSpecification, MaterializationSettings, RecurrenceTrigger, MaterializationComputeResource

card_velocity_features = FeatureSet(
    name="card_velocity_features",
    version="1",
    entities=["card_id"],
    stage="Development",
    specification=FeatureSetSpecification(path="./feature_specs/card_velocity"),
    materialization_settings=MaterializationSettings(
        resource=MaterializationComputeResource(instance_type="Standard_E8s_v3"),
        schedule=RecurrenceTrigger(frequency="Minute", interval=5),
        offline_enabled=True,
        online_enabled=True,
    ),
)
fs_client.feature_sets.begin_create_or_update(card_velocity_features).result()
```

### 6.2 Entities

- `card_id` → velocity, amount-history features
- `customer_id` → behavioral baseline, KYC-linked risk attributes
- `merchant_id` → merchant risk/reputation features
- `device_id` → device trust features

### 6.3 Training-time retrieval (point-in-time join)

```python
from azureml.featurestore import init_online_lookup, get_offline_features

training_df = get_offline_features(
    features=feature_retrieval_spec,
    observation_data=labeled_transactions_df,   # includes event_time + label
    timestamp_column="event_time",
)
```

### 6.4 Inference-time retrieval

At scoring time, the Azure ML **Managed Online Endpoint** scoring script calls the online feature store client with the incoming `card_id`/`customer_id`/`merchant_id`/`device_id`, retrieving the latest materialized feature vector from Azure Managed Redis in single-digit milliseconds, then concatenates it with the transaction's own stateless features before calling the model.

---

## 7. Hybrid Model Architecture

Three model families, trained and served differently, combined into one score:

| Layer | Model | Trained on | Purpose |
|---|---|---|---|
| **Supervised** | XGBoost / LightGBM (gradient-boosted trees) | IEEE-CIS + synthetic-augmented minority class, with class weighting / focal loss (not naive oversampling alone, which overfits duplicated fraud rows) | Learns known fraud patterns with labels |
| **Unsupervised — reconstruction** | Autoencoder (trained only on legitimate transactions) | Streaming Silver data, retrained periodically on recent legitimate traffic | Flags transactions that don't "reconstruct well" — catches novel fraud patterns absent from historical labels |
| **Unsupervised — isolation** | Isolation Forest | Same recent-legitimate-traffic window | Cheap, robust outlier detection as a second unsupervised signal, decorrelated from the autoencoder's failure modes |
| **(Extension) Graph** | GNN or graph-feature-fed booster | Entity graph (Section 5.1) | Catches fraud rings / collusive patterns invisible to any single-transaction model |

**Ensemble combination — stacking meta-learner:** Rather than a fixed weighted blend, use a **stacking meta-learner** (logistic regression or a small gradient-boosted model) that takes the three independently calibrated scores as input features and outputs the final fraud probability:

```python
# Meta-learner training (offline, inside the retraining pipeline — Section 9)
from sklearn.linear_model import LogisticRegression
import numpy as np

meta_features = np.column_stack([
    supervised_calibrated_probs,   # from XGBoost/LightGBM, after Platt/isotonic calibration
    ae_calibrated_scores,          # from Autoencoder reconstruction error, calibrated
    if_calibrated_scores,          # from Isolation Forest, calibrated
])
meta_learner = LogisticRegression(class_weight='balanced', C=0.1)
meta_learner.fit(meta_features, labels)
# Persisted as part of the ensemble artifact in the Azure ML Model Registry
```

**Why a meta-learner over fixed weights:** As fraud patterns evolve, the relative informativeness of each model component shifts — static weights tuned once on a validation set cannot adapt. The meta-learner is retrained alongside the component models in the MLOps pipeline (Section 9), versioned in the model registry as part of the ensemble artifact. It also naturally captures **interaction effects** (e.g., high supervised score + low reconstruction error = likely false positive from the supervised model) that a linear blend misses entirely.

Each component score is calibrated (Platt scaling or isotonic regression) independently before the meta-learner sees them, since raw autoencoder reconstruction error and raw isolation-forest anomaly scores live on incompatible scales from the supervised probability.

**Autoencoder & Isolation Forest retraining protocol:**

- **Cadence:** Weekly retraining, triggered by the drift monitor or on schedule (whichever fires first). Not daily — the autoencoder's reconstruction threshold is sensitive to distribution shifts, and daily retraining risks chasing noise rather than genuine distributional change.
- **"Legitimate" label for retraining:** Use transactions that were **approved by the decision engine AND not subsequently charged back within a 14-day lookback window**. This 14-day delay is inherent and acceptable — the autoencoder is learning the "normal" distribution, not chasing recent fraud patterns (that's the supervised model's job).
- **Contamination guard:** Even with the chargeback filter, assume up to 0.1% of the "legitimate" training window contains undetected fraud. Isolation Forest is robust to this level of label noise. For the autoencoder, use a **trimmed reconstruction loss** (discard the top 1% of per-sample loss values during training) to prevent undetected fraud from widening the learned normality boundary.

**Explainability:** SHAP values computed for the supervised model's contribution — required for regulatory/compliance review of block/decline decisions (a "why was this blocked" answer must exist, not just a score).

**Class imbalance strategy:** real fraud is typically 0.1–0.5% of transactions. Handle via (a) class-weighted loss in the booster, (b) synthetic minority augmentation (your synthetic-fraud-generator source) capped at a reasonable synthetic:real ratio to avoid the model learning generator artifacts, (c) evaluation on **precision-recall AUC and cost-weighted metrics**, never plain accuracy.

---

## 8. Real-Time Inference & Decision Engine

### 8.1 Serving

- **Azure ML Managed Online Endpoint** hosting the ensemble (or **AKS + FastAPI/Triton** if you need custom multi-model orchestration logic beyond what a single endpoint scoring script comfortably does — both are valid; start with Managed Online Endpoints for lower ops burden).
- Endpoint scoring script: fetch online features → assemble vector → run all 3 models → blend → return score + top SHAP contributors.

### 8.2 Latency budget (target <100 ms p99)

| Step | Budget |
|---|---|
| Event Hub → Bronze/Silver → feature update (async, off critical path) | N/A — pre-computed |
| Online feature store lookup (Azure Managed Redis) | ~5–10 ms |
| Model inference (3 models, batched call) | ~20–35 ms |
| Ensemble blend + SHAP top-features | ~5–10 ms |
| Decision engine + network round-trip | ~15–25 ms |
| **Total** | **~50–80 ms**, headroom under the 100ms SLA |

### 8.3 Decision engine

The decision layer is deliberately split across two services, because "approve/block" and "step-up/review" are different kinds of problems — one is a sub-100ms deterministic lookup, the other is a multi-step, sometimes multi-hour, human-involving process:

| Score | Action | Owned by |
|---|---|---|
| < 0.10 | Approve | Azure Function (fast path) |
| 0.10 – 0.60 | Step-up auth (OTP / biometric) | Logic App (workflow) |
| 0.60 – 0.90 | Route to manual review | Logic App (workflow) + Azure SQL (case record) |
| > 0.90 | Block + auto-freeze card, alert customer | Azure Function (fast path) |

> **Threshold calibration note:** The values above (0.10, 0.60, 0.90) are **initial starting points** and must be tuned via cost-sensitive analysis on historical data:
> - Cost of a false negative (missed fraud) ≈ average fraud loss amount
> - Cost of a false positive (blocking legitimate) ≈ customer LTV impact + support cost
> - Cost of a step-up (friction) ≈ conversion drop rate × average transaction value
>
> Thresholds must be stored as **configuration in Azure App Configuration** (not hardcoded in the Function), versioned, and adjustable by fraud-ops without a code deploy. Every threshold change must be logged as an audit event to maintain the regulatory decision trail.

**Azure Function** — stays on the critical inference path (Section 8.2's latency budget). It only ever does two things synchronously: approve, or block-and-freeze. Anything requiring waiting or a human never happens inside the Function.

For the middle two bands, the Function's only job is to publish one message to a **Service Bus Topic** (`fraud-case-events`) and return immediately — this is what keeps a slow downstream step from ever eating into the scoring SLA. A Topic (not a Queue) is the right primitive here because the same event needs to reach three independent consumers without them coupling to each other:

- **Azure Logic App** subscription — owns the actual step-up/review workflow: send OTP → wait for customer response → timeout after N minutes → escalate to manual review → wait for analyst decision → trigger final notification. This is the piece that benefits from being a visible, editable flow rather than Function code — compliance/fraud-ops can change an escalation timeout or add a step without a code deploy, and Logic App's run history doubles as a natural audit trail for "what happened to this case and when."
- **Azure SQL** subscription — inserts/updates the case-management record (case status, assigned analyst, notes, resolution) that an analyst actually works from. This is genuinely a relational, row-level-transaction workload (claim a case, update its status, close it) that Delta's append/MERGE-oriented model isn't built for — Azure SQL is the system of record here, not a copy of transaction data.
- **Compliance/audit logging** subscription — writes an immutable event record for regulatory review, independent of whether the case workflow or the SQL write succeeds.

Net effect: the Function never waits on Logic Apps, Azure SQL, or notification delivery — it publishes and moves on, so a slow OTP provider or a database hiccup can't push the scoring path anywhere near the 100ms budget.

#### Decision-path resilience

- **Service Bus dead-letter queue:** Enable DLQ on each subscription with `maxDeliveryCount = 5` and exponential backoff retry. A dedicated Azure Function monitors the DLQ, logs failures to Application Insights, and raises a P1 alert — a lost fraud case is never acceptable.
- **Idempotency:** The Azure SQL case-management insert must be idempotent on `transaction_id` (MERGE/upsert, not a blind INSERT) since Service Bus guarantees at-least-once delivery, not exactly-once.
- **Poison message handling:** Logic App runs that fail after all retries should park the case in a `review_failed` status in Azure SQL and page the fraud-ops on-call, rather than silently retrying forever.

### 8.4 Fallback & Degradation Strategy

The scoring path has no defined fallback in the original design — a gap that must be closed before production. In a live payment flow, "no response" is worse than a wrong response: it blocks the transaction indefinitely and violates the SLA in the worst possible way.

| Failure mode | Fallback behavior |
|---|---|
| Online feature store (Redis) unreachable | Score using **transaction-level features only** (stateless subset); apply a conservative score bias (+0.15 to final score) to compensate for missing behavioral signals; flag response with `scoring_mode = partial_features` |
| ML endpoint timeout (>100ms) or 5xx | Fall back to a **rules-only engine** (velocity + amount thresholds from a pre-cached rule set in the Azure Function's memory); flag response with `scoring_mode = rules_only` |
| Service Bus unavailable | Azure Function writes the event to an **Azure Storage Queue** (fallback DLQ); a reconciliation job replays to Service Bus once it recovers |
| Full scoring path down | **Default-approve with a hard cap** (e.g., transactions below a fraud-ops-agreed amount threshold only); flag all others for deferred manual review — this threshold is a business decision, not a technical default |

- Implement **circuit breakers** on the Redis and ML endpoint calls within the Azure Function (e.g., using Polly for .NET Functions, or a Python equivalent) with configurable trip thresholds and half-open probe intervals.
- Every decision response must carry a `scoring_mode` field (`full | partial_features | rules_only | default_approve`) so downstream analytics can segment model performance metrics by scoring quality and exclude degraded-mode decisions from drift calculations.

---

## 9. Training & Retraining Pipeline (MLOps)

```
Bronze/Silver historical data
        ↓
Offline feature retrieval (point-in-time join via feature store)
        ↓
Train/val/test split (time-based split, never random — prevents leakage)
        ↓
Azure ML Pipeline: train supervised + retrain autoencoder/IF on recent legit traffic
        ↓
MLflow tracking (native in Azure ML) — metrics, params, artifacts
        ↓
Model evaluation gate (PR-AUC, recall@fixed-FPR, cost-weighted metric ≥ threshold)
        ↓
Azure ML Model Registry (versioned, lineage-linked to training data snapshot)
        ↓
CI/CD (Azure DevOps / GitHub Actions): canary deploy to Managed Online Endpoint
        ↓
Shadow/canary traffic comparison vs. current production model
        ↓
Promote to full traffic (or roll back automatically on regression)
```

- **Time-based splitting** is non-negotiable for fraud: a random split leaks future fraud patterns into training and produces an optimistic, useless offline metric.
- **Drift-triggered retraining:** a scheduled Azure ML monitor (or Evidently AI job) computes PSI (population stability index) on feature distributions and prediction distributions daily against a reference window; crossing a threshold raises an Event Grid event that triggers the retraining pipeline automatically rather than waiting for a scheduled cadence.
- **Canary/shadow deployment** on the Managed Online Endpoint lets the new model score live traffic in parallel without affecting decisions, so its real-world precision/recall is validated before cutover.

---

## 10. Monitoring & Observability

| Layer | Tooling |
|---|---|
| Infra/service health (endpoint latency, error rate, throughput) | Azure Monitor + Application Insights |
| Pipeline job health (Databricks streaming job lag, checkpoint failures) | Databricks job alerts → Azure Monitor Log Analytics |
| Data quality | PyDeequ/Great Expectations metrics → Log Analytics → alert on quarantine-rate spikes |
| Model performance | Precision/recall/AUC/PR-AUC tracked daily in `gold.model_performance_daily`, visualized in Power BI |
| Feature/prediction drift | Evidently AI or Azure ML data drift monitor, PSI per feature |
| Business KPIs | Fraud loss $, false-positive rate (customer friction cost), review-queue SLA — Power BI exec dashboard |

---

## 11. Governance & Security

- **Microsoft Purview**: automated data catalog, lineage from Event Hub → Bronze → Silver → Gold → feature store → model, and PII classification/scanning.
- **Unity Catalog** (if using Databricks Unity Catalog) or Azure ML's RBAC: column-level masking for PII, fine-grained access (e.g., fraud analysts see masked device/IP, compliance sees unmasked with audit logging).
- **Azure Key Vault** for all secrets/connection strings (Event Hubs, Redis, DB credentials) — referenced via Databricks secret scopes / Azure ML workspace connections, never hardcoded.
- **Private endpoints + VNet integration** for Event Hubs, ADLS Gen2, Azure ML workspace, and Redis — no public network exposure for anything in the transaction path.
- **Microsoft Entra ID** for RBAC across all services; managed identities for service-to-service auth (no shared keys where avoidable).
- **PCI-DSS scoping**: PAN tokenization happens upstream of this pipeline; the pipeline only ever handles tokenized `card_id` — this keeps the lakehouse/ML estate out of full PCI cardholder-data-environment scope.
- **Azure SQL** (case management + reference data) uses Transparent Data Encryption and Entra ID authentication, consistent with the rest of the estate — deliberately kept small and single-purpose rather than becoming a second copy of transaction data.
- **Delta Lake transaction log** gives built-in audit trail (time travel) for every Bronze/Silver/Gold table — "what did we know at time T" is always answerable, which matters for regulatory disputes on a specific blocked transaction.

---

## 12. Orchestration & Infrastructure-as-Code

- **Batch orchestration:** Azure Data Factory (IEEE-CIS ingestion, scheduled feature backfills, retraining pipeline triggers, and the Azure SQL → Silver reference-data sync) or Databricks Workflows for Spark-native jobs — pick one as the primary orchestrator to avoid split-brain scheduling; Databricks Workflows is the simpler choice if Databricks is already the compute backbone.
- **Business-process orchestration:** kept deliberately separate from the above — **Azure Logic Apps** owns the step-up/manual-review workflow (Section 8.3). Data pipeline orchestration changes go through the usual CI/CD; the human-facing workflow is meant to be editable by fraud-ops/compliance directly, so mixing it into the data orchestrator would undo that benefit.
- **IaC:** Terraform for all Azure resources (Event Hubs namespace, ADLS Gen2, Databricks workspace, Azure ML workspace + feature store, Managed Redis, AKS if used, Key Vault, Purview) — environment parity between dev/staging/prod, no click-ops.
- **CI/CD:** Azure DevOps or GitHub Actions for both data pipeline code (Spark jobs, dbt-style tests) and ML code (training pipelines, scoring script, endpoint deployment) — separate pipelines, shared artifact versioning.

---

## 13. Technology Stack (Azure-mapped)

| Stage | Azure Service |
|---|---|
| Streaming ingestion | Azure Event Hubs (Premium/Dedicated tier, Capture enabled, Schema Registry) |
| Batch ingestion (IEEE-CIS) | Azure Data Factory |
| Stream + batch processing | Azure Databricks (Structured Streaming, Auto Loader) |
| Lakehouse storage | Delta Lake on Azure Data Lake Storage Gen2 |
| Data quality | PyDeequ / Great Expectations, Unity Catalog constraints |
| Data catalog & lineage | Microsoft Purview |
| Feature store | Azure ML Managed Feature Store (offline: ADLS Gen2/Delta; online: Azure Managed Redis) |
| Graph features | GraphFrames on Databricks, or Azure Cosmos DB (Gremlin API) |
| Model training | Azure ML Pipelines + XGBoost/LightGBM + PyTorch (autoencoder) + scikit-learn (Isolation Forest) |
| Experiment tracking & registry | MLflow (native in Azure ML) |
| Real-time serving | Azure ML Managed Online Endpoints (or AKS + FastAPI/Triton) |
| Decision engine (fast path: approve/block) | Azure Functions |
| Step-up / manual-review workflow | Azure Logic Apps |
| Event fan-out | Azure Service Bus (Topic — multi-subscriber) + Azure Event Grid |
| Case management & operational reference data | Azure SQL Database |
| Drift/monitoring | Evidently AI / Azure ML data drift monitors + Azure Monitor + Application Insights |
| BI / reporting | Azure Synapse (serverless SQL) + Power BI |
| Secrets | Azure Key Vault |
| Identity | Microsoft Entra ID |
| IaC | Terraform |
| CI/CD | Azure DevOps / GitHub Actions |

---

## 14. Suggested Build Order (for a self-study / portfolio implementation)

Given the scope, building this in one shot isn't realistic — a sensible incremental sequence:

0. **Environment & IaC foundation:** Provision the core Azure resources (Resource Group, ADLS Gen2, Databricks workspace, Azure ML workspace, Key Vault, VNet with private endpoints) via Terraform and stand up the CI/CD pipeline skeleton (Azure DevOps or GitHub Actions). This avoids click-ops debt that compounds with every subsequent phase and ensures dev/staging/prod parity from day one. Do not proceed to Phase 1 without this in place.
1. **Batch foundation:** ADLS Gen2 + Databricks, load IEEE-CIS, build Bronze/Silver/Gold for the historical set only. Train a first XGBoost baseline off Gold. No streaming yet.
2. **Streaming path:** stand up Event Hubs, write the Credit Card Transactions replay producer, get raw events flowing into Bronze via Structured Streaming. Validate exactly-once + schema evolution. **Measure actual late-arrival distribution here** to calibrate the watermark before feature engineering.
3. **Feature engineering:** implement windowed/stateful velocity + geo features on the stream; stand up the Azure ML managed feature store with offline+online materialization for one feature set first (prove the point-in-time join works) before adding all feature families. Add the Cosmos DB entity graph sink for real-time local graph queries.
4. **Hybrid model + serving:** train the supervised model on IEEE-CIS + synthetic augmentation; add autoencoder + isolation forest trained on the streaming legitimate traffic (using the 14-day chargeback-filtered legitimate window); train the stacking meta-learner; deploy the ensemble to a Managed Online Endpoint; wire up the Function fast-path (approve/block) **including the fallback/circuit-breaker logic from §8.4**.
5. **Case workflow:** stand up the Service Bus Topic, Azure SQL case-management schema (with idempotent MERGE on `transaction_id`), DLQ monitors, and the Logic App step-up/review workflow — this is the point where the decision engine becomes end-to-end rather than just "returns a score."
6. **MLOps loop:** MLflow tracking, model registry, drift monitors, automated retraining trigger (including meta-learner retraining), canary deployment.
7. **Governance/hardening:** Purview lineage, private endpoints review, Key Vault audit, PCI-scope review, full monitoring dashboards, threshold calibration exercise with fraud-ops.

This order front-loads the parts that de-risk the rest (data correctness, then streaming plumbing) before the more "interesting" ML work — which also happens to be the order that keeps each phase independently demoable.
