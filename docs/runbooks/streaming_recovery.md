# Streaming Pipeline Disaster Recovery Runbook

This runbook documents recovery steps for Structured Streaming query failures or checkpoint corruption.

## Checkpoint Corruption Recovery Procedure

1. **Stop Streaming Job:** Terminate any running instances of `stream_transactions_from_eventhub`.
2. **Find Recovery Timestamp:** Query `bronze.transactions` to identify the last processed timestamp:
   ```sql
   SELECT MAX(_enqueued_time) FROM bronze.transactions;
   ```
3. **Clear Checkpoint Directory:**
   ```python
   dbutils.fs.rm("abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/bronze_streaming_txn/", recurse=True)
   ```
4. **Configure Event Hubs Starting Position:** Set `startingPosition` to the last enqueued timestamp in `eh_conf`.
5. **Restart Streaming Job:** Start the notebook query. The Silver idempotent `MERGE` handles any deduplication automatically.

## Disaster Recovery via Event Hubs Capture Avro Files

If Event Hubs data expires or the stream fails for extended periods, process historical events directly from Capture:
- Source location: `abfss://eventhubs-capture@stfraudlakedev.dfs.core.windows.net/`
- Format: Avro
- Batch Auto Loader job reads from `eventhubs-capture` and appends to `bronze.transactions`.
