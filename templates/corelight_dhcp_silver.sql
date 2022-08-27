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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
WHERE raw:['#path'] = 'dhcp' 
AND raw:client_addr IS NOT NULL
;
