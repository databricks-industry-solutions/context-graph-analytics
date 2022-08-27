-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
-- MAGIC 
-- MAGIC pipeline to build silver & bronze edge tables

-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_dhcp_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT raw:ts AS event_ts,
  rid AS src_rid,
  'ipAddress' AS sub_type,
  raw:client_addr AS sub_id,
  raw:client_addr AS sub_name,
  inline(ARRAY(
    STRUCT(raw:['#path']::string || '-server', raw:['_system_name']::string, raw:['_system_name']::string, raw:['#path']::string, lower(raw:['msg_types[0]'])),
    STRUCT('hostname'::string, raw:host_name::string, raw:host_name::string, 'has', null),
    STRUCT('macAddress'::string, raw:mac::string, raw:mac::string, 'has', null)
  )) AS (obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'dhcp' 
AND raw:client_addr IS NOT NULL
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_dhcp_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_dhcp_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_dns_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT
  raw:ts AS event_ts,
  rid AS src_rid,
  inline(ARRAY(
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string,  'ipAddress', raw:['id.resp_h']::string, raw:['id.resp_h']::string, raw:['#path']::string || '-request', CASE WHEN raw:['@rawstring']:answers IS NULL THEN 'success' ELSE 'failed' END),
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string, 'fqdn', raw:query::string, raw:query::string, raw:['#path']::string || '-query', CASE WHEN raw:['@rawstring']:answers IS NULL THEN 'success' ELSE 'failed' END)
    )) AS (sub_type, sub_id, sub_name, obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'dns'
AND raw:['id.orig_h'] IS NOT NULL

UNION

SELECT
  raw:ts AS event_ts,
  rid AS src_rid,
  inline(
  
    TRANSFORM(
      from_json(raw:['@rawstring']:answers, 'ARRAY<STRING>'),
      x -> struct('ipAddress',
                  raw:['id.resp_h']::string,
                  raw:['id.resp_h']::string,
                  CASE 
                    WHEN startswith(x, 'TXT') THEN 'txt' 
                    WHEN x regexp '(\\d+\\.\\d+\\.\\d+\\.\\d+)' THEN 'ipAddress'
                    ELSE 'fqdn' END,
                  x::string,
                  x::string,
                  'dns-answer'::string, 
                  null::string) 
    )
  ) AS (sub_type, sub_id, sub_name, obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'dns'
AND raw:['@rawstring']:answers IS NOT NULL
AND raw:['id.resp_h'] IS NOT NULL
;

-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_dns_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_dns_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_http_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT
  raw:ts AS event_ts,
  rid AS src_rid,
  inline(
    ARRAY(
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string,  'ipAddress', raw:['id.resp_h']::string, raw:['id.resp_h']::string, raw:['#path']::string || '-' || lower(raw:method), raw:status_code::string),
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string, 'uri', raw:uri::string, raw:uri::string, raw:['#path']::string || '-' || lower(raw:method) ||'-uri', raw:status_code::string),
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string, 'user_agent', raw:user_agent::string, raw:user_agent::string, raw:['#path']::string || '-' || lower(raw:method) ||'-user_agent', raw:status_code::string),
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string, 'referrer', raw:referrer::string, raw:referrer::string, raw:['#path']::string || '-' || lower(raw:method) ||'-referrer', raw:status_code::string)
    )
  ) AS (sub_type, sub_id, sub_name, obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'http'
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_http_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_http_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;

-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_suricata_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT
  raw:ts AS event_ts,
  rid AS src_rid,
  inline(
    ARRAY(
      STRUCT('ipAddress', raw:['id.orig_h']::string, raw:['id.orig_h']::string,  'suricata-alert', raw:['alert.signature_id']::string, raw:['alert.signature']::string, raw:['#path']::string || '-' || lower(raw:service), raw:['alert.severity']::string),
      STRUCT('ipAddress', raw:['id.resp_h']::string, raw:['id.resp_h']::string, 'suricata-alert', raw:['alert.signature_id']::string, raw:['alert.signature']::string, raw:['#path']::string || '-' || lower(raw:service) || '-resp', raw:['alert.severity']::string)
    )
  ) AS (sub_type, sub_id, sub_name, obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'suricata_corelight'
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_suricata_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_suricata_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;

-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_files_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT
  raw:ts AS event_ts,
  rid AS src_rid,
  inline(
    ARRAY(
      STRUCT('ipAddress', raw:['rx_hosts[0]']::string, raw:['rx_hosts[0]']::string,  'hash', 'md5:' || raw:['md5']::string, 'sha1:' || raw:['sha1']::string, 'rx-' || raw:['#path']::string, raw:['source']::string),
      STRUCT('ipAddress', raw:['tx_hosts[0]']::string, raw:['tx_hosts[0]']::string,  'hash', 'md5:' || raw:['md5']::string, 'sha1:' || raw:['sha1']::string, 'tx-' || raw:['#path']::string, raw:['source']::string)
    )
  ) AS (sub_type, sub_id, sub_name, obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.corelight_bronze)
WHERE raw:['#path'] = 'files'
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE corelight_files_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_files_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
-- COMMAND ----------

