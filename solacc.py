from src import graph_pipelines
from src import config
import json
import os
import sys
import click


def get_config():
    cfg_file = "config/cga.json"
    with open(cfg_file, "r") as fp:
        cfg = json.load(fp)
    config.validate_config(cfg)
    return cfg


@click.group()
def cli():
    pass


@cli.command()
def generate():
    cfg = get_config()

    common = {}
    for k, v in cfg.items():
        if type(v) != list and type(v) != dict:
            common[k] = v

    print(f"Generating notebooks for ")
    for nb_spec in cfg["notebooks"]:
        nb_spec.update(common)
        print("... " + nb_spec["id"])
        if nb_spec["id"] == "dlt_edges":
            assert "dlt" in nb_spec and nb_spec["dlt"] == True
            graph_pipelines.gen_dlt_edges_notebook(nb_spec)
        else:
            graph_pipelines.gen_simple_notebook(nb_spec)


@cli.command()
def deploy():
    click.echo("deploy not implemented yet.")


if __name__ == "__main__":
    cli()
