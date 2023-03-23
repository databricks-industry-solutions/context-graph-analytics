# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Threat Analytics Using Context Graphs
# MAGIC 
# MAGIC ## Use cases
# MAGIC 
# MAGIC * An enterprise might have multiple IAM/SSO, AD domains, authn systems including local unix authn. An analyst investigating an incident will need to manually resolve these identities. Context graphs provide same_as edges that capture the results from any entity resolution package and these edges can be traversed like any other relationships.
# MAGIC * Attack Surface Analysis
# MAGIC     * Which user accounts have the largest footprint or surface within the given time frame?
# MAGIC     * Which IP addresses are shared by the largest number of users within the given time frame?
# MAGIC * Impact analysis (blast radius)
# MAGIC     * Given a compromised app, who are the users being affected?
# MAGIC     * Given two compromised user accounts, what if any is the relationship between them within the given time frame?
# MAGIC     * Given a compromiesd app/user/resource, what is the blast radius within the given time frame (use connected component analysis)?
# MAGIC     
# MAGIC ## Reference Architecture
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/Context_Graph_Architecture.png" width="600px">
# MAGIC 
# MAGIC ## Time filtering on time aggregated gold tables
# MAGIC 
# MAGIC * The gold edges tables are aggregated into time buckets at possibly several granularities (hour, day, month etc)
# MAGIC * You typically want to query the gold table with the coarsest time granularity for the retention period your query is relevant for in order to get the best performance. For example if you want to query the data over the past 1 year, you should use the `v_edges_gold_month` (assuming you have constructed the monthly tables).
# MAGIC * Because each row in the gold tables corresponds to a time bucket
# MAGIC     * your query predicate needs to check for intersection with the query time window, AND
# MAGIC     * you might want to aggregate the results so that the edges are unique
# MAGIC     * this is demonstrated in the query below

# COMMAND ----------

# DBTITLE 1,Handling time window filters on the gold aggregated edge tables
ts_lo = "2022-07-19T01:30:00.000+0000"
ts_hi = "2022-07-21T08:50:00.000+0000"

sqlstr = f"""
SELECT e.sub_type, e.sub_id, e.sub_name,
      e.pred, e.pred_status, 
      e.obj_type, e.obj_id, e.obj_name,
      sum(e.cnt) AS cnt, min(e.first_seen) AS first_seen, max(e.last_seen) AS last_seen
FROM solacc_cga.v_edges_day AS e
WHERE  (e.first_seen >= '{ts_lo}' AND e.last_seen <= '{ts_hi}')
    OR (e.first_seen >= '{ts_lo}' AND e.first_seen <= '{ts_hi}') 
    OR (e.last_seen  >= '{ts_lo}' AND e.last_seen <= 'ts_hi') 
GROUP BY sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
ORDER BY first_seen desc
"""

df = spark.sql(sqlstr)
display(df)

# COMMAND ----------

# DBTITLE 1,General statistics of the data
# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   (SELECT count(*) FROM solacc_cga.v_edges_silver ) AS silver_edges_cnt,
# MAGIC   (SELECT count( DISTINCT *) FROM (SELECT sub_id FROM solacc_cga.v_edges_silver UNION ALL SELECT obj_id FROM solacc_cga.v_edges_silver) ) AS silver_nodes_cnt,
# MAGIC   (SELECT count(*) FROM solacc_cga.v_edges_day) AS gold_day_edges_cnt,
# MAGIC   (SELECT count( DISTINCT *) FROM (SELECT sub_id FROM solacc_cga.v_edges_day UNION ALL SELECT obj_id FROM solacc_cga.v_edges_day) ) AS gold_nodes_cnt
# MAGIC   ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT time_bkt, count(*) AS gold_edges_cnt
# MAGIC FROM solacc_cga.v_edges_day
# MAGIC GROUP BY time_bkt;

# COMMAND ----------

# DBTITLE 1,Which users have the largest footprint/surface?
# MAGIC %sql
# MAGIC 
# MAGIC select sub_type, sub_id, sub_name, count(*) as out_degree
# MAGIC from solacc_cga.v_edges_day
# MAGIC where time_bkt = '2022-07-20T00:00:00.000+0000'
# MAGIC group by sub_type, sub_id, sub_name
# MAGIC order by out_degree desc

# COMMAND ----------

# DBTITLE 1,Given a compromised app, which users have logged on in the time window
# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from solacc_cga.v_edges_day
# MAGIC WHERE time_bkt = '2022-07-20T00:00:00.000+0000' and obj_name ilike 'yammer' 

# COMMAND ----------

# DBTITLE 1,Which IP addresses are shared by more than one user account?
# MAGIC %sql
# MAGIC 
# MAGIC SELECT obj_name, count(DISTINCT sub_name) AS users_cnt
# MAGIC FROM solacc_cga.v_edges_day
# MAGIC WHERE time_bkt = '2022-07-20T00:00:00.000+0000' AND obj_type = 'ipAddress'
# MAGIC GROUP BY obj_name
# MAGIC ORDER BY users_cnt DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # The `graphframes` package
# MAGIC 
# MAGIC The `graphframes` package provides out-of-the-box graph algorithms that can be used to analyze very big graphs leveraging the spark dataframes abstraction. 
# MAGIC 
# MAGIC The next two analytics use case uses the graphframes package to perform Breadth First Search (BFS) and Connected Component (CC) analysis over large graph data sets. 
# MAGIC 
# MAGIC ## Recommended Analytics Workflow
# MAGIC 
# MAGIC For very large graphs with billions of edges, we recommend aggregating edges to gold tables using a sensible criteria like time buckets. The gold tables may still contain low billions or millions of edges and should ideally be filtered down to smaller sub-graphs before applying advanced analytics on the filtered graph data.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/multiresolution_graph_analytics.png" width="800px">
# MAGIC 
# MAGIC ## Reachability or Path Analysis using Breadth First Search (BFS)
# MAGIC 
# MAGIC BFS is used to determined if there is some path that connects to compromised user accounts. The paths (if any) represents possible pathways that an actor might have moved laterally.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/bfs_paths.png" width="600px">

# COMMAND ----------

# DBTITLE 1,Given two compromised accounts, what if any is the relationship between them? (Uses the same_as edges)
from graphframes import *

v = spark.sql("""
SELECT sub_id AS id, sub_name AS name
FROM solacc_cga.v_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT obj_id AS id, obj_name AS name
FROM solacc_cga.v_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT sub_id AS id, sub_name AS name
FROM solacc_cga.same_as
""")

# duplicate the edges in the reverse direction in order to enable undirected path finding
e = spark.sql("""
SELECT sub_id AS src, obj_id AS dst, pred AS relationship
FROM solacc_cga.v_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT obj_id AS src, sub_id AS dst, 'rev-' || pred AS relationship
FROM solacc_cga.v_edges_day
WHERE time_bkt = '2022-07-20T00:00:00.000+0000'
UNION
SELECT sub_id AS src, obj_id AS dst, pred AS relationship
FROM solacc_cga.same_as
""")

#display(e)

g = GraphFrame(v, e)

# use breadth first search to find a path between the two compromise accounts
paths = g.bfs("name = 'megan.chang@chang-fisher.com'", "name = 'maria.cook@summers.info'", maxPathLength=7)

display(paths)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Impact Analysis using the Connected Components (CC) Algorithm
# MAGIC 
# MAGIC CC analysis is used to determine the partition/segmentation of the attack surface represented in the graph that represents the blast radius if any node in each component is compromised.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/cc_impact.png" width="600px">
# MAGIC 
# MAGIC Note that connected component algorithm applies to undirected graphs and finds the components/subgraphs that are connected internally but not connected externally to the subgraph. The analogue for directed graphs is the notion of strongly connected components (SCC) where the nodes within a SCC are reachable to each other.

# COMMAND ----------

# DBTITLE 1,Impact zone analysis
spark.sparkContext.setCheckpointDir("/tmp/solacc_cga/checkpoints")

cc = g.connectedComponents()
display(cc)

# COMMAND ----------

# DBTITLE 1,The number of connected components (impact zones) and their sizes
from pyspark.sql import functions as f

print("Number of connected components = ", end="")
display(cc.select("component").distinct().count())

print("\n\nNumber of nodes in each connected component:")
display(cc.groupBy("component").count().orderBy(f.col("count").desc()))

# COMMAND ----------


