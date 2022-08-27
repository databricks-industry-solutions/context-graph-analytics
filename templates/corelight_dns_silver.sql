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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
WHERE raw:['#path'] = 'dns'
AND raw:['@rawstring']:answers IS NOT NULL
AND raw:['id.resp_h'] IS NOT NULL
;

