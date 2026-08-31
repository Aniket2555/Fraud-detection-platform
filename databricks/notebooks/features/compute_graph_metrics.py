# Databricks notebook
# GraphFrames Batch Feature Extraction
#
# Historized snapshot table (fix applied — see Phase 3 "Known Issues"):
# previously this did a full `mode("overwrite")` of the entire table on
# every run, keeping only the latest centrality/community values. Now each
# run's output is appended keyed on (entity_id, entity_type, snapshot_date),
# partitioned by snapshot_date, so training rows can PIT-join against the
# graph metrics that were current at event_time instead of today's.

from pyspark.sql.functions import col, lit, current_timestamp, current_date
from delta.tables import DeltaTable
from graphframes import GraphFrame

# customer_id/card_id/device_id only exist in the Event Hub-streamed
# dataset, not the Phase 1 IEEE-CIS batch table.
silver = spark.table("fraud_detection_dev.silver.streaming_transactions")

cust_vertices = silver.select(col("customer_id").alias("id")).distinct().withColumn("type", lit("customer"))
card_vertices = silver.select(col("card_id").alias("id")).distinct().withColumn("type", lit("card"))
device_vertices = (
    silver
    .filter((col("device_id").isNotNull()) & (col("device_id") != "unknown"))
    .select(col("device_id").alias("id")).distinct()
    .withColumn("type", lit("device"))
)

vertices = cust_vertices.union(card_vertices).union(device_vertices)

edges_card = (
    silver
    .select(col("customer_id").alias("src"), col("card_id").alias("dst"))
    .distinct().withColumn("relationship", lit("OWNS_CARD"))
)
edges_device = (
    silver
    .filter((col("device_id").isNotNull()) & (col("device_id") != "unknown"))
    .select(col("customer_id").alias("src"), col("device_id").alias("dst"))
    .distinct().withColumn("relationship", lit("USED_DEVICE"))
)
edges = edges_card.union(edges_device)

gf = GraphFrame(vertices, edges)

pagerank_results = gf.pageRank(resetProbability=0.15, maxIter=5)
degree_df = gf.degrees

# sparkContext.setCheckpointDir() is a raw Hadoop-FileSystem RDD checkpoint
# (needed by connectedComponents() to truncate lineage) -- it bypasses Unity
# Catalog's external-location/credential system entirely, needs classic
# storage-account-key auth for abfss:// (not configured here, DBFS root is
# disabled), and Databricks doesn't support UC Volumes for this specific
# low-level API either. Local disk works because this cluster is
# single-node (num_workers=0) -- driver and "executor" are the same
# machine, so there's no need for a distributed/shared filesystem here.
spark.sparkContext.setCheckpointDir("file:///tmp/graphframes-checkpoints/")
cc_results = gf.connectedComponents()

graph_metrics = (
    pagerank_results.vertices
    .select(col("id").alias("entity_id"), col("type").alias("entity_type"), col("pagerank").alias("graph_pagerank_score"))
    .join(degree_df.select(col("id"), col("degree").alias("graph_degree_centrality")), col("entity_id") == col("id"), "left")
    .drop("id")
    .join(cc_results.select(col("id"), col("component").alias("graph_community_id")), col("entity_id") == col("id"), "left")
    .drop("id")
    .withColumn("snapshot_date", current_date())
    .withColumn("feature_timestamp", current_timestamp())
)

target_table_name = "fraud_detection_dev.gold.graph_entity_metrics"

if spark.catalog.tableExists(target_table_name):
    target = DeltaTable.forName(spark, target_table_name)
    (
        target.alias("t")
        .merge(
            graph_metrics.alias("s"),
            "t.entity_id = s.entity_id AND t.entity_type = s.entity_type AND t.snapshot_date = s.snapshot_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        graph_metrics.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .saveAsTable(target_table_name)
    )

print(f"✅ Graph features materialized: {graph_metrics.count()} entities for snapshot_date.")
