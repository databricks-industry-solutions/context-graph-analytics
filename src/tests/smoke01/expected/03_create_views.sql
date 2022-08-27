-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
-- MAGIC 
-- MAGIC create views (one for each time granularity) to unify the gold edges for each source

-- COMMAND ----------


DROP VIEW IF EXISTS solacc_cga.v_edges_DAY;

CREATE VIEW IF NOT EXISTS solacc_cga.v_edges_DAY 
AS

SELECT 'okta' AS src, * FROM solacc_cga.okta_edges_gold_day

UNION ALL

SELECT 'aad' AS src, * FROM solacc_cga.aad_edges_gold_day

;

-- COMMAND ----------


DROP VIEW IF EXISTS solacc_cga.v_edges_silver;

CREATE VIEW IF NOT EXISTS solacc_cga.v_edges_silver 
AS

SELECT 'okta' AS src, * FROM solacc_cga.okta_edges_silver

UNION ALL

SELECT 'aad' AS src, * FROM solacc_cga.aad_edges_silver

;

-- COMMAND ----------

