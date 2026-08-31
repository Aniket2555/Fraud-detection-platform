# 04 — Event Hubs, Transaction Producer, Streaming Ingestion

**Starting state:** Event Hubs namespace existed (Terraform) but was
empty; the transaction producer had never been run against real
infrastructure; no streaming notebooks had executed.

**End state:** Event Hubs live with real replayed traffic from the
ULB/IEEE credit-card dataset; Bronze streaming ingestion consuming it;
late-arrival distribution measured to calibrate Phase 3's watermark.

## Event Hubs links

| Thing | Value |
|---|---|
| Namespace | `ehns-fraud-dev` |
| Event Hub | `eh-transactions` |
| Azure Portal | Resource Groups > `rg-fraud-detection-dev` > `ehns-fraud-dev` |

## Storing the Event Hubs connection secrets in Key Vault

```bash
# scripts/store_eventhub_secrets.sh — fixed to resolve the KV name
# dynamically (bug #1) rather than a hardcoded "kv-fraud-dev"
KV_NAME=$(az keyvault list --resource-group rg-fraud-detection-dev --query "[0].name" -o tsv)
CONN_STR=$(az eventhubs eventhub authorization-rule keys list \
  --resource-group rg-fraud-detection-dev \
  --namespace-name ehns-fraud-dev \
  --eventhub-name eh-transactions \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)
az keyvault secret set --vault-name "$KV_NAME" --name eventhub-connection-string --value "$CONN_STR"
```

## The transaction producer

`producers/transaction_producer/config.py` originally hardcoded
`time_anchor` to a fixed past date (bug #21) — meaningless for measuring
live late-arrival behavior, since every event would already be "old" by
definition. Fixed to default to `datetime.now(timezone.utc)`, overridable
via a `TIME_ANCHOR` env var for reproducible replay testing.

The producer replays the Kaggle ULB `creditcard.csv` dataset (time-
compressed) as if it were live traffic, preserving relative inter-arrival
gaps at a configurable speed-up factor.

```bash
export EVENTHUB_CONNECTION_STRING="$(az keyvault secret show --vault-name <kv> --name eventhub-connection-string --query value -o tsv)"
export DATASET_PATH=/tmp/ulb-creditcard/creditcard.csv
.venv/Scripts/python.exe -m producers.transaction_producer.producer \
  --speed-factor 1000 --total-events 20000
```

Ran in the background (`run_in_background: true`); sent 20,000/20,000
events successfully. **Process note:** partway through, I mistakenly
concluded via an empty harness output-tracking file that the process was
"stuck" and killed it with `kill -9` — it was actually healthy and had
already sent 10,000 events. Root cause: I had also manually redirected
stdout to my own log file, so the harness's own tracking file legitimately
had nothing in it — I was checking the wrong file. Lesson: when you add
your own redirect, check *that* file, not the harness's own output
tracker. The producer was simply restarted from where it left off (Kafka/
EventHub offset semantics meant no data was lost on the consumer side).

## Bronze streaming ingestion (consumer side)

`databricks/notebooks/bronze/` also has a streaming ingestion path (reads
from Event Hubs directly via the `azure-eventhubs-spark` Maven connector,
rather than Auto Loader/files) that writes into
`bronze.streaming_transactions_raw`. Run via the Command Execution API,
or manually as a Databricks notebook — this one is meant to run
continuously (structured streaming), not once.

### Measuring late-arrival distribution

Compared each event's Event Hubs `enqueuedTime` against the payload's own
embedded transaction timestamp:

```python
from pyspark.sql import functions as F
df = spark.table("fraud_detection_dev.bronze.streaming_transactions_raw")
df.withColumn("lateness_sec", F.col("enqueued_time").cast("long") - F.col("event_time_ts").cast("long")) \
  .select(F.expr("percentile_approx(lateness_sec, array(0.5, 0.9, 0.99))").alias("p50_p90_p99")) \
  .show(truncate=False)
```

This distribution directly informed the watermark chosen in Phase 3's
streaming silver transform (`stream_silver_from_bronze.py`) — set
generously above the observed p99 to avoid dropping legitimately-late
events while still bounding state size.

## Verifying it worked

```sql
SELECT COUNT(*) FROM fraud_detection_dev.bronze.streaming_transactions_raw;
-- grows continuously while the streaming query + producer are both running
```

```bash
databricks clusters list --output json | python -c "import json,sys; [print(c['cluster_id'], c['state']) for c in json.load(sys.stdin)['clusters']]"
```

Streaming query health, from within a notebook cell:
```python
for q in spark.streams.active:
    print(q.name, q.status)
```

## To reproduce this from scratch

1. Store the Event Hubs connection string in Key Vault (script above).
2. Start the cluster if terminated.
3. Start the Bronze streaming ingestion notebook (it'll block/run
   continuously — leave it running, or launch it as a Databricks Job for
   a persistent run).
4. Run the transaction producer, pointed at the same Event Hub.
5. Query `bronze.streaming_transactions_raw` to confirm row counts are
   climbing.
6. Run the late-arrival query above once enough events have landed.
