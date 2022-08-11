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
# MAGIC ## Analysis using the `graphframes` package 
# MAGIC 
# MAGIC The next two analytics use case uses the graphframes package to perform Breadth First Search (BFS) and Connected Component (CC) analysis over large graph data sets.
# MAGIC 
# MAGIC * BFS is used to determined if there is some path that connects to compromised user accounts. The paths (if any) represents possible pathways that an actor might have moved laterally.
# MAGIC * CC analysis is used to determine the partition/segmentation of the attack surface represented in the graph that represents the blast radius if any node in each component is compromised.

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

