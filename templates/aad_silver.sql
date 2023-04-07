CREATE STREAMING LIVE TABLE aad_edges_silver
PARTITIONED BY (event_date, pt)
TBLPROPERTIES("quality"="silver")
AS
SELECT event_ts,
  event_date,
  left(raw:userId, 1) as pt,
  rid AS src_rid,
  'user-aad' AS sub_type,
  raw:userId AS sub_id,
  raw:userPrincipalName AS sub_name,
  inline(array(
    struct('app', raw:appId::string, raw:appDisplayName::string, 'uses', null),
    struct('ipAddress', raw:ipAddress::string, raw:ipAddress::string, 'uses', null),
    struct('resource', raw:resourceId::string, raw:resourceDisplayName::string, 'signin', case when raw:status.errorCode = 0 then 'success' else 'failed' end)
  )) AS (obj_type, obj_id, obj_name, pred, pred_status)
FROM STREAM({{tgt_db_name}}.aad_bronze);


