CREATE STREAMING LIVE TABLE okta_edges_silver
PARTITIONED BY (event_date, pt)
TBLPROPERTIES("quality"="silver")
AS
SELECT event_ts,
  event_date,
  left(raw:actor.id, 1) as pt,
  rid as src_rid,
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
FROM STREAM({{tgt_db_name}}.okta_bronze)
WHERE raw:eventType IN ('user.authentication.auth_via_mfa', 'user.authentication.sso', 'user.authentication.auth_via_social');

