# Databricks notebook source
# This notebook is designed to be run as a task within a multi-task job workflow.
# These time window input widgets enable the user to do back fill and re-processing within the multi-task job workflow
#dbutils.widgets.removeAll()
dbutils.widgets.text("okta_start_time", "", "start time (YYYY-mm-ddTHH:MM:SSZ): ")
start_time = dbutils.widgets.get("okta_start_time")
dbutils.widgets.text("okta_end_time", "", "end time (YYYY-mm-ddTHH:MM:SSZ): ")
end_time = dbutils.widgets.get("okta_end_time")

print(start_time + " to " + end_time)


# COMMAND ----------

import json

#Here we use Okta API token, in production we recommend storing this token in Databricks secret store
#https://docs.databricks.com/security/secrets/index.html
cfg = {
  "base_url": "https://dev-74006068.okta.com/api/v1/logs",
  "token": "CHANGEME",
  "start_time": start_time,
  "end_time": end_time,
  "batch_size": 1000,
  "target_db": "",
  "target_table": "okta_bronze",
  "storage_path": "/tmp/solacc_cga"
}

# we need to figure out where/when we execute this DDL. Ideally we don't want it in the collector task. This is needed to enable querying of the bronze table to figure out what is the latest event timestamp
sql_str = f"""
CREATE TABLE IF NOT EXISTS {cfg['target_db']}.{cfg['target_table']} (
            ingest_ts TIMESTAMP,
            event_ts TIMESTAMP,
            event_date TIMESTAMP,
            raw STRING) USING DELTA PARTITIONED BY (event_date) LOCATION '{cfg['storage_path']}'
            """
print(sql_str)
spark.sql(sql_str)

# if task parameters (ie widgets) are empty, then we default to using the latest timestamp from the bronze table
if len(cfg["start_time"])==0 and len(cfg["end_time"])==0:
  sql_str = f"""
    select max(event_ts) as latest_event_ts
    from {cfg['target_db']}.{cfg['target_table']}"""

  df = spark.sql(sql_str)
  latest_ts = df.first()["latest_event_ts"]
  if latest_ts is None:
    print("latest_ts is none - default to 7 days from now")
    default_ts = datetime.today() - timedelta(days=7) 
    cfg["start_time"]=default_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
  else:
    print("latest_ts from bronze table is " + latest_ts.isoformat())
    cfg["start_time"]=latest_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    
print(json.dumps(cfg, indent=2))

# COMMAND ----------

import requests
import json
import re
import datetime

from pyspark.sql import Row
import pyspark.sql.functions as f

def poll_okta_logs(cfg, debug=False):
  MINIMUM_COUNT=5 # Must be >= 2, see note below

  headers = {'Authorization': 'SSWS ' + cfg["token"]}
  query_params = { 
                   "limit": str(cfg["batch_size"]),
                   "sortOrder": "ASCENDING",
                   "since": cfg["start_time"]
                 }
  if cfg["end_time"]:
    query_params["until"] = cfg["end_time"]

  url = cfg["base_url"]
  total_cnt = 0
  while True:
    # Request the next link in our sequence:
    r = requests.get(url, headers=headers, params=query_params)

    if not r.status_code == requests.codes.ok:
      break
  
    ingest_ts = datetime.datetime.now(datetime.timezone.utc)
    
    # Break apart the records into individual rows
    jsons = []
    jsons.extend([json.dumps(x) for x in r.json()])
  
    # Make sure we have something to add to the table
    if len(jsons) == 0: break
    # Load into a dataframe
    df = (
      sc.parallelize([Row(raw=x) for x in jsons]).toDF()
        .selectExpr(f"'{ingest_ts.isoformat()}'::timestamp AS ingest_ts",
                  "date_trunc('DAY', raw:published::timestamp) AS event_date",
                  "raw:published::timestamp AS event_ts",
                  "raw AS raw")
    )
    #print("%d %s" % (df.count(),url))
    total_cnt += len(jsons)
    if debug:
      display(df)
    else:
      # Append to delta table
      df.write\
       .option("mergeSchema", "true")\
       .format('delta') \
       .mode('append') \
       .partitionBy("event_date") \
       .save(cfg["storage_path"])
    
    #When we make an API call, we cause an event.  So there is the potential to get
    #into a self-perpetuating loop.  Thus we look to ensure there is a certain minimum number
    #of entries before we are willing loop again.
    if len(jsons) < MINIMUM_COUNT: break
  
    #print(r.headers["Link"])
  
    # Look for the 'next' link; note there is also a 'self' link, so we need to get the right one
    rgx = re.search(r"\<([^>]+)\>\; rel=\"next\"", str(r.headers['Link']), re.I)
    if rgx:
      # We got a next link match; set that as new URL and repeat
      url = rgx.group(1)
      continue
    else:
      # No next link, we are done
      break
  return total_cnt
  
cnt = poll_okta_logs(cfg)
print(f"Total records polled = {cnt}")

# COMMAND ----------

