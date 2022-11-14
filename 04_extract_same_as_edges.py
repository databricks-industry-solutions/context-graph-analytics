# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Extract `same_as` edges
# MAGIC 
# MAGIC * For now, simulate entity resolution using hard coded same_as edges
# MAGIC * TODO: use zingg to create the same_as edges

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS solacc_cga.same_as;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS solacc_cga.same_as (
# MAGIC   src string,
# MAGIC   time_bkt timestamp,
# MAGIC   sub_type string,
# MAGIC   sub_id string,
# MAGIC   sub_name string,
# MAGIC   pred string,
# MAGIC   pred_status string,
# MAGIC   obj_type string,
# MAGIC   obj_id string,
# MAGIC   obj_name string
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Add simple resolutions where the entities share the same email address
# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE solacc_cga.same_as;
# MAGIC 
# MAGIC INSERT OVERWRITE solacc_cga.same_as 
# MAGIC (
# MAGIC   WITH entity_mapping AS
# MAGIC   (
# MAGIC     SELECT DISTINCT okta.sub_type AS src_type, okta.sub_id AS src_id, aad.sub_type AS tgt_type, aad.sub_id AS tgt_id, aad.sub_name AS name
# MAGIC     FROM solacc_cga.okta_edges_gold_day AS okta join solacc_cga.aad_edges_gold_day AS aad on okta.sub_name = aad.sub_name
# MAGIC   )
# MAGIC   SELECT 'okta' AS src, NULL AS time_bkt, src_type AS sub_type, src_id AS sub_id, name AS sub_name, 'same_as' AS pred, NULL AS pred_status, tgt_type AS obj_type, tgt_id AS obj_id, name AS obj_name
# MAGIC   FROM entity_mapping
# MAGIC   UNION
# MAGIC   SELECT 'aad' AS src, NULL AS time_bkt, tgt_type AS sub_type, tgt_id AS sub_id, name AS sub_name, 'same_as' AS pred, NULL AS pred_status, src_type AS obj_type, src_id AS obj_id, name AS obj_name
# MAGIC   FROM entity_mapping
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add a non-trivial identity resolutions
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO solacc_cga.same_as 
# MAGIC (
# MAGIC SELECT DISTINCT 'aad' AS src, NULL AS time_bkt, src.sub_type, src.sub_id, src.sub_name, 'same_as' AS pred, NULL AS pred_status, tgt.sub_type, tgt.sub_id, tgt.sub_name
# MAGIC FROM
# MAGIC (
# MAGIC SELECT DISTINCT sub_type, sub_id, sub_name
# MAGIC FROM solacc_cga.aad_edges_gold_day
# MAGIC WHERE sub_name = 'maria.cole@chang-fisher.com' 
# MAGIC OR sub_name = 'maria.cook@summers.info'
# MAGIC ) AS src, 
# MAGIC (
# MAGIC SELECT DISTINCT sub_type, sub_id, sub_name
# MAGIC FROM solacc_cga.aad_edges_gold_day
# MAGIC WHERE sub_name = 'maria.cole@chang-fisher.com' 
# MAGIC OR sub_name = 'maria.cook@summers.info'
# MAGIC ) AS tgt
# MAGIC WHERE src.sub_id != tgt.sub_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM solacc_cga.same_as

# COMMAND ----------

