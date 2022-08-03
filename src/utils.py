import os

# wraps a SQL str into a databricks SQL notebook
def wrap_sql_notebook(sql_str_list, desc):
  return f"""-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Table Pipeline
-- MAGIC 
-- MAGIC {desc}

-- COMMAND ----------
{'-- COMMAND ----------'.join(sql_str_list)}
-- COMMAND ----------

"""

def gen_bronze_table_name(data_src_type, db_name=None):
  prefix = ""
  if db_name is not None:
    prefix=f"{db_name}."
  return f"{prefix}{data_src_type}_bronze"

def gen_silver_table_name(data_src_type, db_name=None):
  prefix = ""
  if db_name is not None:
    prefix=f"{db_name}."
  return f"{prefix}{data_src_type}_silver"

def gen_notebook_file_name(data_src_type, nb_type, dir_name=None):
  fname = f"{data_src_type}.{nb_type}"
  if dir_name is not None:
    return os.path.join(dir_name, fname)
  return fname
