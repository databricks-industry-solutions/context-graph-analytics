# Databricks notebook source
# MAGIC %md 
# MAGIC # Notebook for setting up sample bronze table
# MAGIC 
# MAGIC * limited to Azure AD and Okta data for now

# COMMAND ----------

cfg={
  "storage_path": "/tmp/solacc_cga",
  "db_name": "solacc_cga",
  "table_list": [ 
                  {
                     "name": "aad_bronze",
                     "ts_path": "createdDateTime",
                     "files": ["./data/aad-i-sample.json.gz", "./data/aad-ni-sample.json.gz"]
                  }, 
                  {
                    "name": "okta_bronze",
                    "ts_path": "published",
                    "files": ["./data/okta-sample.json.gz"]
                  }
               ]
}

def getParam(parm):
  return cfg.get(parm)

sql_list = [ f"""
DROP SCHEMA IF EXISTS {getParam('db_name')} CASCADE
""",
f"""
CREATE SCHEMA IF NOT EXISTS {getParam('db_name')} LOCATION '{getParam('storage_path')}'
"""]

for t in getParam("table_list"):
  table_name = getParam('db_name') + "." + t["name"]
  sql_list.append(
            f"""
CREATE TABLE IF NOT EXISTS {table_name} (
  ingest_ts TIMESTAMP, 
  event_ts TIMESTAMP,
  event_date DATE,
  rid STRING,
  raw STRING
)
USING DELTA
PARTITIONED BY (event_date)
""")

for sql_str in sql_list:
  print(sql_str)
  spark.sql(sql_str)


# COMMAND ----------

import json
import gzip
import datetime
import dateutil
from pyspark.sql import Row

def load_jsonfiles(full_table_name, jsonfiles, ts_path):
  rec_cnt = 0
  for jsonfile in jsonfiles:
    with gzip.open(jsonfile, "r") as fp:
      data_str = fp.read()
      data = json.loads(data_str)
  
    rec_cnt += len(data)
    ingest_ts = datetime.datetime.now(datetime.timezone.utc)

    df = (
          sc.parallelize([Row(raw=json.dumps(x)) for x in data]).toDF()
          .selectExpr(f"'{ingest_ts.isoformat()}'::timestamp AS ingest_ts",
                  f"date_trunc('DAY', raw:{ts_path}::timestamp)::date AS event_date",
                  f"raw:{ts_path}::timestamp AS event_ts",
                  "uuid() AS rid",
                  "raw AS raw")
        )
    df.write.mode("append").saveAsTable(full_table_name)
  return rec_cnt

for t in getParam("table_list"):
  full_table_name = getParam('db_name') + "." + t["name"]
  load_jsonfiles(full_table_name, t["files"], t["ts_path"])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from solacc_cga.okta_bronze

# COMMAND ----------

