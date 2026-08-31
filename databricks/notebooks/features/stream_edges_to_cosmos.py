# Databricks notebook
# Cosmos DB Gremlin Edge Writer

import nest_asyncio
nest_asyncio.apply()  # gremlin_python's client runs its own event loop, which
                       # conflicts with the notebook kernel's already-running one

from gremlin_python.driver import client as gremlin_client, serializer
from pyspark.sql.functions import col

COSMOS_ENDPOINT = dbutils.secrets.get(scope="kv-fraud", key="cosmos-gremlin-endpoint")
COSMOS_KEY = dbutils.secrets.get(scope="kv-fraud", key="cosmos-gremlin-key")


def upsert_edge(g_client, src_id, src_type, dst_id, dst_type, relationship):
    # .all().result() makes each submit() synchronous and surfaces errors --
    # without it, submit() only starts an async op and returns immediately,
    # so the addE below could run before its addV calls actually complete,
    # and any failure would be silently dropped.
    g_client.submit(
        f"g.V('{src_id}').fold().coalesce(unfold(), addV('{src_type}').property('id', '{src_id}').property('partitionKey', '{src_id}'))"
    ).all().result()
    g_client.submit(
        f"g.V('{dst_id}').fold().coalesce(unfold(), addV('{dst_type}').property('id', '{dst_id}').property('partitionKey', '{dst_id}'))"
    ).all().result()
    g_client.submit(
        f"g.V('{src_id}').coalesce(outE('{relationship}').where(inV().hasId('{dst_id}')), addE('{relationship}').to(g.V('{dst_id}')))"
    ).all().result()


def process_batch(batch_df, batch_id):
    rows = batch_df.select("customer_id", "card_id", "device_id").collect()
    g_client = gremlin_client.Client(
        COSMOS_ENDPOINT, "g",
        username="/dbs/graph-fraud-db/colls/entity-graph",
        password=COSMOS_KEY,
        # Cosmos DB's Gremlin API only supports GraphSON 2.0 -- the
        # client's default (3.0) connects successfully but the server
        # then closes the connection on the first real request.
        message_serializer=serializer.GraphSONSerializersV2d0(),
    )
    try:
        for row in rows:
            upsert_edge(g_client, row.customer_id, "customer", row.card_id, "card", "OWNS_CARD")
            if row.device_id and row.device_id != "unknown":
                upsert_edge(g_client, row.customer_id, "customer", row.device_id, "device", "USED_DEVICE")
    finally:
        g_client.close()


silver_stream = spark.readStream.table("fraud_detection_dev.silver.streaming_transactions")
(
    silver_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "abfss://checkpoints@stfraudlakedev.dfs.core.windows.net/cosmos_edge_writer/")
    .trigger(processingTime="5 minutes")
    .start()
)
