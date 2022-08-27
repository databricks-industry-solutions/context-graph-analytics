CREATE STREAMING LIVE TABLE corelight_dhcp_edges_gold_{{time_granularity}}
PARTITIONED BY (time_bkt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('{{time_granularity}}', event_ts) as time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.corelight_dhcp_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
