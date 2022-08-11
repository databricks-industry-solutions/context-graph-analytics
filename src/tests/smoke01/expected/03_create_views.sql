-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
-- MAGIC 
-- MAGIC create views (one for each time granularity) to unify the gold edges for each source

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS solacc_cga.v_edges_DAY 
AS

SELECT * FROM solacc_cga.okta_edges_gold_day

UNION ALL

SELECT * FROM solacc_cga.aad_edges_gold_day

;

-- COMMAND ----------

