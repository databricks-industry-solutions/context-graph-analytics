# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Graph Analytics Without a Graph Database!
# MAGIC 
# MAGIC * Data: Okta logs and AAD logs
# MAGIC * Schema-on-read 
# MAGIC     * the graph data model will NOT be materialized
# MAGIC     * use views to extract the edges
# MAGIC     * use views to aggregate the edges for data reduction
# MAGIC * Use graph algorithms from `graphframes`: BFS & ConnectedComponent for big data graphs!
# MAGIC 
# MAGIC ## Recommended Analytics Workflow
# MAGIC 
# MAGIC For very large graphs with billions of edges, we recommend aggregating edges for data reduction using a sensible criteria like time buckets. The aggregated view may still contain low billions or millions of edges and should ideally be filtered down to smaller sub-graphs before applying advanced analytics on the filtered graph data.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/schema_on_read_graph_analytics.png" width="1000px">
# MAGIC 
# MAGIC 
# MAGIC ## First let's take a peek at the data

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema {{tgt_db_name}};

# COMMAND ----------

# DBTITLE 1,Okta logs
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM okta_bronze;

# COMMAND ----------

# DBTITLE 1,AAD logs
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM aad_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use views to extract edges from the logs and aggregate the edges for data reduction

# COMMAND ----------

# DBTITLE 1,View for extracting edges from okta logs
# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_okta_edges;
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS v_okta_edges
# MAGIC AS
# MAGIC SELECT event_ts,
# MAGIC   rid as src_rid,
# MAGIC   'user-okta' as sub_type,
# MAGIC   raw:actor.id as sub_id,
# MAGIC   raw:actor.alternateId as sub_name,
# MAGIC   inline(
# MAGIC     array_union(
# MAGIC       array(
# MAGIC         struct('ipAddress'::string, raw:client.ipAddress::string, raw:client.ipAddress::string, 'uses'::string, null::string)
# MAGIC       ),
# MAGIC       transform(
# MAGIC         filter( arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ), x-> x['1']!='User' and x['1']!='AppUser' ), x -> struct(
# MAGIC                         x['1']::string,
# MAGIC                         x['0']::string,
# MAGIC                         x['2']::string,
# MAGIC                         'signin'::string, 
# MAGIC                         case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end) 
# MAGIC       )
# MAGIC     )
# MAGIC   ) as (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa', 'user.authentication.sso', 'user.authentication.auth_via_social');
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM v_okta_edges
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,View for aggregating the okta edges for data reduction
# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_okta_edges_day;
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS v_okta_edges_day
# MAGIC AS
# MAGIC SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
# MAGIC   min(event_ts) as first_seen,
# MAGIC   max(event_ts) as last_seen,
# MAGIC   count(*) as cnt
# MAGIC FROM v_okta_edges
# MAGIC WHERE obj_id IS NOT NULL
# MAGIC GROUP BY time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
# MAGIC ;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM v_okta_edges_day;

# COMMAND ----------

# DBTITLE 1,View for extracting edges from AAD logs
# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS v_aad_edges;
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS v_aad_edges
# MAGIC AS
# MAGIC SELECT event_ts,
# MAGIC   rid AS src_rid,
# MAGIC   'user-aad' AS sub_type,
# MAGIC   raw:userId AS sub_id,
# MAGIC   raw:userPrincipalName AS sub_name,
# MAGIC   inline(array(
# MAGIC     struct('app', raw:appId::string, raw:appDisplayName::string, 'uses', null),
# MAGIC     struct('ipAddress', raw:ipAddress::string, raw:ipAddress::string, 'uses', null),
# MAGIC     struct('resource', raw:resourceId::string, raw:resourceDisplayName::string, 'signin', case when raw:status.errorCode = 0 then 'success' else 'failed' end)
# MAGIC   )) AS (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM aad_bronze;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM v_aad_edges
# MAGIC LIMIT 100;

# COMMAND ----------

# DBTITLE 1,View for aggregating the AAD edges for data reduction
# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS v_aad_edges_day;
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS v_aad_edges_day
# MAGIC AS
# MAGIC SELECT date_trunc('DAY', event_ts) as time_bkt,
# MAGIC   sub_type, sub_id, sub_name,
# MAGIC   pred, pred_status,
# MAGIC   obj_type, obj_id, obj_name,
# MAGIC   min(event_ts) as first_seen,
# MAGIC   max(event_ts) as last_seen,
# MAGIC   count(*) as cnt
# MAGIC FROM v_aad_edges
# MAGIC WHERE obj_id IS NOT NULL
# MAGIC GROUP BY time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM v_aad_edges_day
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 'okta_edges' as view_type, count(*) as cnt FROM v_okta_edges
# MAGIC UNION ALL
# MAGIC SELECT 'okta_edges_day' as view_type, count(*) as cnt FROM v_okta_edges_day
# MAGIC UNION ALL
# MAGIC SELECT 'aad_edges' as view_type, count(*) as cnt FROM v_aad_edges
# MAGIC UNION ALL
# MAGIC SELECT 'aad_edges_day' as view_type, count(*) as cnt FROM v_aad_edges_day

# COMMAND ----------

# DBTITLE 1,View for combining all the aggregated edges from different data sources.
# MAGIC %sql
# MAGIC 
# MAGIC DROP VIEW IF EXISTS vv_edges_day;
# MAGIC 
# MAGIC CREATE VIEW IF NOT EXISTS vv_edges_day 
# MAGIC AS
# MAGIC SELECT 'okta' AS src, * FROM v_okta_edges_day
# MAGIC UNION ALL
# MAGIC SELECT 'aad' AS src, * FROM v_aad_edges_day
# MAGIC ;
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM vv_edges_day
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## 1. Reachability or Path Analysis using Breadth First Search (BFS)
# MAGIC 
# MAGIC BFS is used to determined if there is some path that connects to compromised user accounts. The paths (if any) represents possible pathways that an actor might have moved laterally.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/bfs_paths.png" width="600px">

# COMMAND ----------

# DBTITLE 1,Step 1: Use a query on the views to extract the relevant subgraph (vertices & edges)
v = spark.sql("""
SELECT sub_id AS id, sub_name AS name
FROM vv_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT obj_id AS id, obj_name AS name
FROM vv_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
""")

# duplicate the edges in the reverse direction in order to enable undirected path finding
e = spark.sql("""
SELECT sub_id AS src, obj_id AS dst, pred AS relationship
FROM vv_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT obj_id AS src, sub_id AS dst, 'rev-' || pred AS relationship
FROM vv_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
""")

display(v)
display(e)

# COMMAND ----------

# DBTITLE 1,Step 2: Run the Breadth-First Search (bfs) method
from graphframes import *

g = GraphFrame(v, e)

# use breadth first search to find a path between the two compromise accounts
paths = g.bfs("name = 'megan.chang@chang-fisher.com'", "name = 'maria.cook@summers.info'", maxPathLength=7)

display(paths)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2. Impact Analysis using the Connected Components (CC) Algorithm
# MAGIC 
# MAGIC CC analysis is used to determine the partition/segmentation of the attack surface represented in the graph that represents the blast radius if any node in each component is compromised.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/cc_impact.png" width="600px">
# MAGIC 
# MAGIC Note that connected component algorithm applies to undirected graphs and finds the components/subgraphs that are connected internally but not connected externally to the subgraph. The analogue for directed graphs is the notion of strongly connected components (SCC) where the nodes within a SCC are reachable to each other.

# COMMAND ----------

# DBTITLE 1,Run the connected components method
spark.sparkContext.setCheckpointDir("{{storage_path}}/checkpoints")

cc = g.connectedComponents()
display(cc)

# COMMAND ----------

# DBTITLE 1,Examine the connected components
from pyspark.sql import functions as f

print("Number of connected components = ", end="")
display(cc.select("component").distinct().count())

print("\n\nNumber of nodes in each connected component:")
display(cc.groupBy("component").count().orderBy(f.col("count").desc()))

# COMMAND ----------


