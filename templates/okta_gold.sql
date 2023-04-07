CREATE STREAMING LIVE TABLE okta_edges_gold_{{time_granularity}}
PARTITIONED BY (time_bkt, pt)
TBLPROPERTIES("quality"="gold")
AS
SELECT date_trunc('{{time_granularity}}', event_ts) as time_bkt,
  left(sub_id, 1) as pt,
  sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name,
  min(event_ts) as first_seen,
  max(event_ts) as last_seen,
  count(*) as cnt
FROM STREAM(LIVE.okta_edges_silver)
WHERE obj_id IS NOT NULL
group by time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name
;
