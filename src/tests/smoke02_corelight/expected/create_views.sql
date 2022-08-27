-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
-- MAGIC 
-- MAGIC create views (one for each time granularity) to unify the gold edges for each source

-- COMMAND ----------


DROP VIEW IF EXISTS solacc_cga.v_edges_DAY;

CREATE VIEW IF NOT EXISTS solacc_cga.v_edges_DAY 
AS

SELECT 'corelight_dhcp' AS src, * FROM solacc_cga.corelight_dhcp_edges_gold_day

UNION ALL

SELECT 'corelight_dns' AS src, * FROM solacc_cga.corelight_dns_edges_gold_day

UNION ALL

SELECT 'corelight_http' AS src, * FROM solacc_cga.corelight_http_edges_gold_day

UNION ALL

SELECT 'corelight_suricata' AS src, * FROM solacc_cga.corelight_suricata_edges_gold_day

UNION ALL

SELECT 'corelight_files' AS src, * FROM solacc_cga.corelight_files_edges_gold_day

;

-- COMMAND ----------


DROP VIEW IF EXISTS solacc_cga.v_edges_silver;

CREATE VIEW IF NOT EXISTS solacc_cga.v_edges_silver 
AS

SELECT 'corelight_dhcp' AS src, * FROM solacc_cga.corelight_dhcp_edges_silver

UNION ALL

SELECT 'corelight_dns' AS src, * FROM solacc_cga.corelight_dns_edges_silver

UNION ALL

SELECT 'corelight_http' AS src, * FROM solacc_cga.corelight_http_edges_silver

UNION ALL

SELECT 'corelight_suricata' AS src, * FROM solacc_cga.corelight_suricata_edges_silver

UNION ALL

SELECT 'corelight_files' AS src, * FROM solacc_cga.corelight_files_edges_silver

;

-- COMMAND ----------

