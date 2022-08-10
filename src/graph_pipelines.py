from jinja2 import Template
import json
import os

# wraps a SQL str into a databricks SQL notebook
def wrap_cmds_into_notebook(cmd_str_list, desc):
    sep = "\n-- COMMAND ----------\n"
    return f"""-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Auto-generated Notebook
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


def write_notebook(nb_spec, nb_str):
    # generate the path of the output & write out
    (out_dir, out_file) = get_notebook_file_path(nb_spec)
    os.makedirs(out_dir, exist_ok=True)
    with open(out_file, "w") as outfp:
        outfp.write(nb_str)
    return out_file


def gen_simple_notebook(nb_spec):
    # generate the path to template file
    template_file = get_template_file_path(nb_spec)
    with open(template_file, "r") as fp:
        template_str = fp.read()

    # construct the variables for render
    t = Template(template_str)
    nb_str = t.render(nb_spec)
    out_file = write_notebook(nb_spec, nb_str)

    return nb_str


def gen_dlt_edges_notebook(nb_spec):

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

    out_file = write_notebook(nb_spec, nb_str)

    return out_file

def gen_create_views_notebook(nb_spec):

    cmd_list = []
    for time_granularity in nb_spec["gold_agg_buckets"]:
        union_list = []
        view_name = f"{nb_spec['tgt_db_name']}.v_edges_{time_granularity}"
        for src in nb_spec["data_sources"]:
            # generate the gold table name
            gold_table =  f"{nb_spec['tgt_db_name']}.{src}_edges_gold_{time_granularity}"
            union_list.append(f"""
SELECT * FROM {gold_table}
""")

        union_str = "\nUNION ALL\n".join(union_list)
        nb_str = f"""
CREATE VIEW IF NOT EXISTS {view_name} 
AS
{union_str}
;
"""
        cmd_list.append(nb_str)

    nb_str = wrap_cmds_into_notebook(cmd_list, nb_spec["desc"])

    out_file = write_notebook(nb_spec, nb_str)

    return out_file



if __name__ == "__main__":
    pass
