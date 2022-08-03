from . import utils
from jinja2 import Template
import json
import os

# wraps a SQL str into a databricks SQL notebook
def wrap_cmds_into_notebook(cmd_str_list, desc):
  sep = '\n-- COMMAND ----------\n'
  return f"""-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Table Pipeline
-- MAGIC 
-- MAGIC {desc}

-- COMMAND ----------
{sep.join(cmd_str_list)}
-- COMMAND ----------

"""

# for template fragments
def get_template_cmd_file_path(nb_spec, cmd_id):
  return f"{nb_spec['template_dir']}/{cmd_id}.{nb_spec['type']}"

# for simple notebook templates
def get_template_file_path(nb_spec):
  return f"{nb_spec['template_dir']}/{nb_spec['id']}.{nb_spec['type']}"

def get_notebook_file_path(nb_spec):
  out_dir = f"{nb_spec['deploy_dir']}"
  out_path = os.path.join(out_dir, f"{nb_spec['id']}.{nb_spec['type']}")
  return (out_dir, out_path)

def gen_simple_notebook(nb_spec):
  # generate the path to template file
  template_file = get_template_file_path(nb_spec)
  with open(template_file, "r") as fp:
    template_str = fp.read()

  # construct the variables for render
  t = Template(template_str)
  nb_str = t.render(nb_spec)
  
  # generate the path of the output & write out
  (out_dir, out_file) = get_notebook_file_path(nb_spec)
  os.makedirs(out_dir, exist_ok=True)
  with open(out_file, "w") as outfp:
    outfp.write(nb_str)
  return out_file

def gen_dlt_notebook(nb_spec):

  cmd_list = []
  for src in nb_spec["data_sources"]:
    # for silver tables
    # generate the path to template file
    template_file = get_template_cmd_file_path(nb_spec, f"{src}_silver")
    with open(template_file, "r") as fp:
      template_str = fp.read()
    # construct the variables for render
    t = Template(template_str)
    nb_str = t.render(nb_spec)

    cmd_list.append(nb_str)

    for time_granularity in nb_spec["gold_agg_buckets"]:
      # generate the path to template file
      template_file = get_template_cmd_file_path(nb_spec, f"{src}_gold")
      with open(template_file, "r") as fp:
        template_str = fp.read()
      # construct the variables for render
      nb_spec["time_granularity"] = time_granularity
      t = Template(template_str)
      nb_str = t.render(nb_spec)
      cmd_list.append(nb_str)

  nb_str = wrap_cmds_into_notebook(cmd_list, nb_spec["desc"])
  # generate the path of the output & write out
  (out_dir, out_file) = get_notebook_file_path(nb_spec)
  os.makedirs(out_dir, exist_ok=True)
  with open(out_file, "w") as outfp:
    outfp.write(nb_str)
  return out_file


     
#  with open(mapping_file, "r") as fp:
#    cfg = json.load(fp)
#  template_file = f"templates/normalizer.{dlt_type}"
#  with open(template_file, "r") as fp:
#    template_str = fp.read()
#
#  db_name = cga_cfg["tgt_db_name"]
#  src_table = utils.gen_bronze_table_name(ntype, db_name)
#  # DLT live table names cannot include schema/database name
#  tgt_table = utils.gen_silver_table_name(ntype)
#  mapping = cfg["field_mappings"]
#  dlt_dir = os.path.join(cga_cfg["deploy_dir"], "normalizers")
#  dlt_file = utils.gen_notebook_file_name(ntype, dlt_type, dlt_dir)
#
#  dlt_sql_str = gen_normalizer_sql(src_table, mapping, tgt_table, template_str)
#  nb_str = utils.wrap_sql_notebook([dlt_sql_str], f"{ntype} normalizer")
#
#  os.makedirs(dlt_dir, exist_ok=True)
#  with open(dlt_file, "w") as outfp:
#    outfp.write(nb_str)
  return "hello"


def gen_normalizer_sql(bronze_table, mapping, silver_table, template_str):
  select_list = []
  for (tgt, src) in mapping.items():
    select_list.append(f"\n  {src} AS {tgt}")
  
  t = Template(template_str)
  sql_str = t.render( dlt_silver_table = silver_table,
                    field_mappings   = ",".join(select_list),
                    src_bronze_table = bronze_table
                    )
  return sql_str

if __name__ == "__main__":
  pass
