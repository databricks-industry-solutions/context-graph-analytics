import graph_pipelines
import config
import json
import os
import sys
import click


def get_config(cfg_file):
    try:
        with open(cfg_file, "r") as fp:
            cfg = json.load(fp)
    except IOError:
        print (f"Error opening config file '{cfg_file}'")
        return None
    config.validate_config(cfg)
    return cfg


@click.group()
@click.option('--config', default="config/cga.json", help="use config from given json file")
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    cfg = get_config(config)
    if cfg is None:
        sys.exit()
    ctx.obj['cfg']= cfg


@cli.command()
@click.option('--prefix', default=False, help="prefix generated notebooks with a number")
@click.pass_context
def generate(ctx, prefix):
    cfg = ctx.obj["cfg"]

    # extract common key-atomic_value pairs to be used in templates for each notebook
    common = {}
    for k, v in cfg.items():
        if type(v) != list and type(v) != dict:
            common[k] = v

    i = 0
    print(f"Generating notebooks for ")
    for nb_spec in cfg["notebooks"]:
        # add the top-level common vars into nb_spec for used in template rendering
        nb_spec.update(common)
        nb_spec["prefix"] = ""
        if prefix and nb_spec["id"] != "RUNME":
            nb_spec["prefix"] = f"{i:02d}_"
        print(f"... {nb_spec['prefix']}{nb_spec['id']}")
        if nb_spec["id"] == "dlt_edges":
            assert "dlt" in nb_spec and nb_spec["dlt"] == True
            graph_pipelines.gen_dlt_edges_notebook(nb_spec)
        elif nb_spec["id"] == "create_views":
            graph_pipelines.gen_create_views_notebook(nb_spec)
        else:
            graph_pipelines.gen_simple_notebook(nb_spec)
        i += 1


@cli.command()
@click.pass_context
def deploy(ctx):
    #print(f"deploy.cfg= {json.dumps(ctx.obj['cfg'], indent=2)}")
    click.echo("deploy not implemented yet.")

if __name__ == "__main__":
    cli(obj={})
