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
FROM STREAM({{tgt_db_name}}.corelight_bronze)
WHERE raw:['#path'] = 'files'
;
