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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
WHERE raw:['#path'] = 'http'
;
