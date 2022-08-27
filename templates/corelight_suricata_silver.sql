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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
WHERE raw:['#path'] = 'suricata_corelight'
;
