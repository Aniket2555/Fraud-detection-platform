# 05 — Feature Engineering, GraphFrames, Cosmos DB Graph

**Starting state:** feature-computation notebooks existed but pointed at
the wrong (Phase 1 batch) table, and `materialize_feature_store.py` was
entirely commented out. Cosmos DB Gremlin edge-writing had never
connected successfully even once.

**End state:** behavioral baselines, merchant risk, and graph metrics all
computed off the real streaming table; feature store materialized; a real
transaction/card/merchant graph live in Cosmos DB Gremlin, with degree
centrality, PageRank, and connected-components features flowing back.

## The wrong-table bug (bugs #22-25)

`stream_silver_from_bronze.py` was originally merging live Event-Hub data
into the *same* `silver.transactions` table the Phase 1 batch pipeline
had already populated from the static IEEE-CIS Kaggle CSVs — two
incompatible schemas (different columns, different semantics for
`event_date` vs `event_time_ts`). This wasn't just wrong output — it
actively corrupted the Phase 1 baseline table on every micro-batch.

Fixed by routing streaming writes to a new table,
`silver.streaming_transactions`, and updating every downstream
feature notebook (`compute_behavioral_baselines.py`,
`compute_merchant_risk.py`, `compute_graph_metrics.py`) to read from it
instead. Also fixed along the way:
- `compute_behavioral_baselines.py` referenced a nonexistent
  `event_date` column on the streaming table — the streaming table only
  has `event_time_ts` (a real timestamp, not a pre-truncated date).
- `compute_merchant_risk.py` filtered `is_fraud == 1` against what is
  actually a boolean column — fixed to `is_fraud == True`.

## Running the feature notebooks

Via the Databricks Command Execution API, or manually as notebooks
attached to `batch-etl-dev`:

1. `databricks/notebooks/features/compute_behavioral_baselines.py`
2. `databricks/notebooks/features/compute_merchant_risk.py`
3. `databricks/notebooks/features/compute_graph_metrics.py`
4. `databricks/notebooks/features/materialize_feature_store.py`

### `materialize_feature_store.py` (bug #31)

Was entirely commented out (matching the repo's own `TODO.md` admission)
and additionally referenced the wrong table when uncommented. Rewritten
to actually join behavioral, merchant-risk, and graph features into the
`gold.feature_store` table, keyed by `transaction_id`, ready for the
Phase 4 training pipeline's point-in-time join.

## GraphFrames: connectedComponents() checkpoint bug (bug #26)

`connectedComponents()` internally requires
`sparkContext.setCheckpointDir(...)`, which is a raw Spark-context-level
directory setting — it doesn't go through Unity Catalog's governed table/
volume paths at all, and DBFS root is disabled on UC-enabled workspaces.
Fixed by pointing the checkpoint dir at **local disk** on the driver
(`file:/tmp/graphframes-checkpoints`), which only works because the
cluster is single-node (no distributed filesystem needed to make local
paths consistent across executors).

```python
spark.sparkContext.setCheckpointDir("file:/tmp/graphframes-checkpoints")
```

## Cosmos DB Gremlin: streaming edge writer

`stream_edges_to_cosmos.py` had three separate connectivity/correctness
bugs (#27-30):

1. **Wrong secret scope name** — referenced `fraud-secrets`, the actual
   scope created in `04-streaming.md`/`03-data-landing.md` is `kv-fraud`.
2. **Event loop conflict** — `gremlin_python`'s client manages its own
   asyncio event loop, but the notebook kernel already has one running.
   Fixed with `nest_asyncio.apply()` at the top of the notebook.
3. **GraphSON version mismatch** — Cosmos DB's Gremlin API only speaks
   GraphSON 2.0; `gremlin_python`'s default client serializer is 3.0. The
   symptom was subtle: the connection succeeds, but the server silently
   closes it on the first real request. Fixed by passing
   `GraphSONSerializersV2d0()` explicitly when constructing the client.
4. **Fire-and-forget writes** — `upsert_edge()`'s three Gremlin
   submissions (`g.V(...)`, `g.V(...)`, `g.addE(...)`) were dispatched
   without waiting on `.all().result()`, creating a real race (an edge
   could be submitted before its vertices existed) and silently
   swallowing any server-side errors. Fixed to synchronously await each
   step.

### Cosmos DB Gremlin connection info

```bash
COSMOS_ACCOUNT=cosmos-fraud-dev-604t
az cosmosdb show --name $COSMOS_ACCOUNT --resource-group rg-fraud-detection-dev \
  --query documentEndpoint -o tsv
# Gremlin endpoint is the same account, port 443, wss:// (gremlinpython
# constructs this from the account name automatically)
```

The Gremlin key was fetched once, with explicit user approval (Gremlin's
wire protocol has no scoped-RBAC alternative — it's master-key-only),
and stored in the `kv-fraud` secret scope, never printed to the
transcript.

### Verifying it worked

Query the graph directly (via a notebook cell using the same
`gremlin_python` client):
```python
g.V().count().next()          # vertex count growing
g.E().count().next()          # edge count growing
```

```python
for q in spark.streams.active:
    print(q.name, q.status)   # confirm the edge-writer stream is running, not stalled
```

### Graph metrics back into features

`compute_graph_metrics.py` runs PageRank, connected components, and
degree centrality over the (batch snapshot of the) graph using
GraphFrames directly on the Silver Delta table — not by reading back from
Cosmos — since GraphFrames needs a Spark DataFrame representation
regardless. Cosmos DB serves the graph for *online*/serving-time lookups
(e.g., "is this card connected to a known fraud ring right now"); the
batch GraphFrames computation is for offline feature engineering.

## To reproduce this from scratch

1. Ensure the streaming silver table (`04-streaming.md`) is populated.
2. Run `compute_behavioral_baselines.py`, `compute_merchant_risk.py`.
3. Set the local-disk checkpoint dir, then run `compute_graph_metrics.py`.
4. Run `materialize_feature_store.py`.
5. Start `stream_edges_to_cosmos.py` as a continuous streaming job (needs
   the transaction producer from `04-streaming.md` running to have
   anything to write).
6. Verify vertex/edge counts climbing in Cosmos DB via the Gremlin
   console or a notebook cell.
