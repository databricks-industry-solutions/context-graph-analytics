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
# MAGIC CREATE TABLE IF NOT EXISTS solacc_cga.same_as (
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
# MAGIC     select distinct okta.sub_type as src_type, okta.sub_id as src_id, aad.sub_type as tgt_type, aad.sub_id as tgt_id, aad.sub_name as name
# MAGIC     from solacc_cga.okta_edges_gold_day AS okta join solacc_cga.aad_edges_gold_day AS aad on okta.sub_name = aad.sub_name
# MAGIC   )
# MAGIC   SELECT src_type AS sub_type, src_id AS sub_id, name AS sub_name, 'same_as' AS pred, NULL as pred_status, tgt_type AS obj_type, tgt_id AS obj_id, name AS obj_name
# MAGIC   FROM entity_mapping
# MAGIC   UNION
# MAGIC   SELECT tgt_type AS sub_type, tgt_id AS sub_id, name AS sub_name, 'same_as' AS pred, NULL as pred_status, src_type AS obj_type, src_id AS obj_id, name AS obj_name
# MAGIC   FROM entity_mapping
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add a non-trivial identity resolutions
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC INSERT INTO solacc_cga.same_as 
# MAGIC (
# MAGIC select distinct src.sub_type, src.sub_id, src.sub_name, 'same_as' AS pred, NULL as pred_status, tgt.sub_type, tgt.sub_id, tgt.sub_name
# MAGIC FROM
# MAGIC (
# MAGIC select distinct sub_type, sub_id, sub_name
# MAGIC from solacc_cga.aad_edges_gold_day
# MAGIC where sub_name = 'maria.cole@chang-fisher.com' 
# MAGIC or sub_name = 'maria.cook@summers.info'
# MAGIC ) AS src, 
# MAGIC (
# MAGIC select distinct sub_type, sub_id, sub_name
# MAGIC from solacc_cga.aad_edges_gold_day
# MAGIC where sub_name = 'maria.cole@chang-fisher.com' 
# MAGIC or sub_name = 'maria.cook@summers.info'
# MAGIC ) AS tgt
# MAGIC where src.sub_id != tgt.sub_id
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from solacc_cga.same_as

# COMMAND ----------

