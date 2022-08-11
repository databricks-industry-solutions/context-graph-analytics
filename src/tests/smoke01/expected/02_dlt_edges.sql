-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
-- MAGIC 
-- MAGIC pipeline to build silver & bronze edge tables

-- COMMAND ----------
CREATE STREAMING LIVE TABLE okta_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT event_ts,
  raw:uuid as src_rid,
  'user-okta' as sub_type,
  raw:actor.id as sub_id,
  raw:actor.alternateId as sub_name,
  inline(
    array_union(
      array(
        struct('ipAddress'::string, raw:client.ipAddress::string, raw:client.ipAddress::string, 'uses'::string, null::string)
      ),
      transform(
        filter( arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ), x-> x['1']!='User' and x['1']!='AppUser' ), x -> struct(
                        x['1']::string,
                        x['0']::string,
                        x['2']::string,
                        'signin'::string, 
                        case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end) 
      )
    )
  ) as (obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.okta_bronze)
WHERE raw:eventType IN ('user.authentication.auth_via_mfa', 'user.authentication.sso', 'user.authentication.auth_via_social');

-- COMMAND ----------
CREATE STREAMING LIVE TABLE okta_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.okta_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
-- COMMAND ----------
CREATE STREAMING LIVE TABLE aad_edges_silver
PARTITIONED BY (event_ts)
TBLPROPERTIES("quality"="silver")
AS
SELECT event_ts,
  raw:id AS src_rid,
  'user-aad' AS sub_type,
  raw:userId AS sub_id,
  raw:userPrincipalName AS sub_name,
  inline(array(
    struct('app', raw:appId::string, raw:appDisplayName::string, 'uses', null),
    struct('ipAddress', raw:ipAddress::string, raw:ipAddress::string, 'uses', null),
    struct('resource', raw:resourceId::string, raw:resourceDisplayName::string, 'signin', case when raw:status.errorCode = 0 then 'success' else 'failed' end)
  )) AS (obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM(solacc_cga.aad_bronze);


-- COMMAND ----------
CREATE STREAMING LIVE TABLE aad_edges_gold_DAY
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('DAY', event_ts) as time_bkt,
  sub_type, sub_id, sub_name,
  pred, pred_status,
  obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.aad_edges_silver)
WHERE obj_id IS NOT NULL
GROUP BY time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
-- COMMAND ----------

