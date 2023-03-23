# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## How to map any JSON to a Graph Data Model
# MAGIC 
# MAGIC Whether you are doing schema-on-read or schema-on-write, you still need to map your source data schemas to a graph data model.
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/graph_data_model.png" width="800px">

# COMMAND ----------

# MAGIC %sql
# MAGIC use schema {{tgt_db_name}};

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raw
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa')
# MAGIC LIMIT 5
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Pretty print one JSON raw string
import json

df = spark.sql(f"""
SELECT raw
FROM okta_bronze
WHERE raw:eventType IN ('user.authentication.auth_via_mfa')
LIMIT 1""")

(json_str,) = df.first()

obj = json.loads(json_str)
print(json.dumps(obj, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1-to-1: Extracting 1 source row to 1 target edge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_ts,
# MAGIC   rid as src_rid,
# MAGIC   'user-okta' as sub_type,
# MAGIC   raw:actor.id as sub_id,
# MAGIC   raw:actor.alternateId as sub_name,
# MAGIC   'ipAddress'::string as obj_type, 
# MAGIC   raw:client.ipAddress::string as obj_id, 
# MAGIC   raw:client.ipAddress::string as obj_name,
# MAGIC   'uses'::string as pred,
# MAGIC   null::string as pred_status
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa')
# MAGIC LIMIT 5
# MAGIC ;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## 1-to-many: Extracting 1 source row to multiple edges with the same subject

# COMMAND ----------

# DBTITLE 1,Use inline(), array(), struct()
# MAGIC %sql
# MAGIC SELECT event_ts,
# MAGIC   rid as src_rid,
# MAGIC   'user-okta' as sub_type,
# MAGIC   raw:actor.id as sub_id,
# MAGIC   raw:actor.alternateId as sub_name,
# MAGIC   inline(
# MAGIC     array(
# MAGIC         struct('ipAddress'::string, raw:client.ipAddress::string, raw:client.ipAddress::string, 'uses'::string, null::string)
# MAGIC     )
# MAGIC   ) as (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa')
# MAGIC LIMIT 5
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Let's extract the geolocation of the client as an edge as well
# MAGIC %sql
# MAGIC SELECT event_ts,
# MAGIC   rid as src_rid,
# MAGIC   'user-okta' as sub_type,
# MAGIC   raw:actor.id as sub_id,
# MAGIC   raw:actor.alternateId as sub_name,
# MAGIC   inline(
# MAGIC     array(
# MAGIC         struct('ipAddress'::string, raw:client.ipAddress::string, raw:client.ipAddress::string, 'uses'::string, null::string),
# MAGIC         struct('city_state'::string, raw:client.geographicalContext.postalCode::string,
# MAGIC               raw:client.geographicalContext.city::string || ',' || raw:client.geographicalContext.state::string || ',' || raw:client.geographicalContext.country::string,
# MAGIC               'located_at'::string, null::string)
# MAGIC     )
# MAGIC   ) as (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa')
# MAGIC LIMIT 5
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What if there is an array in the json and we want to create an edge for each array element?
# MAGIC 
# MAGIC We will use some SQL high-order functions!
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/graph_data_model_mapping.png" width="800px">

# COMMAND ----------

# DBTITLE 1,Extract the fields in the array of records that we want
# MAGIC %sql
# MAGIC SELECT
# MAGIC from_json(raw:target[*].id, 'array<string>') AS target_ids, 
# MAGIC from_json(raw:target[*].type, 'array<string>') AS target_types, 
# MAGIC from_json(raw:target[*].displayName, 'array<string>') AS target_names
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');

# COMMAND ----------

# DBTITLE 1,Stitch the extracted fields back into an array of structs
# MAGIC %sql
# MAGIC SELECT
# MAGIC arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ) as extracted
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');

# COMMAND ----------

# DBTITLE 1,We want to add some extra fields to each struct
# MAGIC %sql
# MAGIC SELECT
# MAGIC transform( 
# MAGIC   arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ),
# MAGIC   x -> struct(
# MAGIC     x['1']::string,
# MAGIC     x['0']::string,
# MAGIC     x['2']::string,
# MAGIC     'signin'::string, 
# MAGIC     case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end
# MAGIC     )
# MAGIC ) as extracted
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');

# COMMAND ----------

# DBTITLE 1,Now we use inline to turn the array of structs into rows
# MAGIC %sql
# MAGIC SELECT
# MAGIC inline(
# MAGIC transform( 
# MAGIC   arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ),
# MAGIC   x -> struct(
# MAGIC     x['1']::string,
# MAGIC     x['0']::string,
# MAGIC     x['2']::string,
# MAGIC     'signin'::string, 
# MAGIC     case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end
# MAGIC     )
# MAGIC )
# MAGIC ) AS (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');

# COMMAND ----------

# DBTITLE 1,But we don't want the obj_type = User, let's filter those out
# MAGIC %sql
# MAGIC SELECT
# MAGIC inline(
# MAGIC transform( 
# MAGIC   filter(
# MAGIC     arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ), 
# MAGIC     x-> x['1']!='User' and x['1']!='AppUser'),
# MAGIC   x -> struct(
# MAGIC     x['1']::string,
# MAGIC     x['0']::string,
# MAGIC     x['2']::string,
# MAGIC     'signin'::string, 
# MAGIC     case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end
# MAGIC     )
# MAGIC )
# MAGIC ) AS (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');

# COMMAND ----------

# DBTITLE 1,Putting it altogether
# MAGIC %sql
# MAGIC SELECT event_ts,
# MAGIC   rid as src_rid,
# MAGIC   'user-okta' as sub_type,
# MAGIC   raw:actor.id as sub_id,
# MAGIC   raw:actor.alternateId as sub_name,
# MAGIC   inline(
# MAGIC     array_union(
# MAGIC       array(
# MAGIC         struct('ipAddress'::string, raw:client.ipAddress::string, raw:client.ipAddress::string, 'uses'::string, null::string),
# MAGIC         struct('city_state'::string, raw:client.geographicalContext.postalCode::string,
# MAGIC               raw:client.geographicalContext.city::string || ',' || raw:client.geographicalContext.state::string || ',' || raw:client.geographicalContext.country::string,
# MAGIC               'located_at'::string, null::string)
# MAGIC       ),
# MAGIC       transform(
# MAGIC         filter( arrays_zip( from_json(raw:target[*].id, 'array<string>'), from_json(raw:target[*].type, 'array<string>'), from_json(raw:target[*].displayName, 'array<string>') ), x-> x['1']!='User' and x['1']!='AppUser' ), x -> struct(
# MAGIC                         x['1']::string,
# MAGIC                         x['0']::string,
# MAGIC                         x['2']::string,
# MAGIC                         'signin'::string, 
# MAGIC                         case when raw:outcome.result = 'SUCCESS' then 'success'::string else 'failed'::string end) 
# MAGIC       )
# MAGIC     )
# MAGIC   ) as (obj_type, obj_id, obj_name, pred, pred_status)
# MAGIC FROM okta_bronze
# MAGIC WHERE raw:eventType IN ('user.authentication.auth_via_mfa');
# MAGIC --, 'user.authentication.sso', 'user.authentication.auth_via_social');

# COMMAND ----------


