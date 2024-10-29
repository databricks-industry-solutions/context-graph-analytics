<img src="https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png" width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Context Graph Analytics
___
Contact Author: <cybersecurity@databricks.com>

## Use Cases

Personas: SOC analyts, Incident Responders, Threat Hunters

* Impact analysis (blast radius):
    * Given a compromised user account/apps/ip address, what other user accounts, apps, ip addresses etc. are affected across the enterprise?
* Lateral movement investigation
    * How do you correlate multiple log sources to investigate an attacker moving from one system to another?
* Insider threats investigation
    * How do you leverage HR data together with security logs to find suspicious insider threat activity?
* Query across different data sources:
    * How do you leverage entity resolution modules (eg. Zingg, splink) to query over multiple sources without knowing how each entity is represented in each source?
* Comprehensive attack surface management
* Fusion-center style fraud detection

___

## Reference Architecture

<img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/Context_Graph_Architecture.png" width="600px">

___

## Technical Overview

This solution accelerator:
* assumes that the raw data from security appliances (EDR, IAM, Network etc.) is already being collected or exported to bronze tables;
* provides a sample collector notebook for polling okta data via okta's REST API for reference, but collection logic is not the focus of this solution accelerator;
* provides sample DLT pipelines to extract a property graph model in the form of edges or triples into silver tables and further aggregates the silver tables via different time buckets into gold tables;
* provides a notebook to create the unifying set of edge/triple views that are used to query all the edges;
* provides a notebook that demonstrates how Entity resolution packages can be used to produce `same_as` edges and how these `same_as` edges can be used in the graph analytics to resolve and investigate different entities even though the entities are represented differently in different data sets;
* provides a notebook that demonstrates how graph navigation can be used in an investigation; 
* provides a demo UI that demonstrate how the graph traversals can be used in an investigation workflow for analysts with no programming experience. 
* ... tbd ... more to come

___
## Quickstart Guide

If you just want to try out the solution accelerator in a Databricks workspace without any additional customization in terms of target database names etc.:

1. Ensure you have git integration setup in the workspace 
2. Clone this repo to the workspace
3. Switch to the `test_drive` branch. The deploy folder will contain a generic version of the generated notebooks.
4. Run the `RUNME.py` notebook.
5. Feel free to open up the `analytics_01_impact.py` or the `analytics_02_investigation.py` notebook and play with the analytics.

If you are curious what does `RUNME.py` do, it basically automates the following manual steps:

1. Run the `bronze_sample.py` notebook to load the sample bronze tables
2. Setup the DLT pipeline job for the edge extraction & aggregation pipelines using the `dlt_edges.sql` notebook.
3. Run the DLT pipeline job.
4. Run the `create_views.py` notebook.
5. Run the `extract_same_as_edges.py` notebook.
6. Run the `analytics_01_impact.py`.

If you want to customize or further build on the solution accelerator, follow the deployment guide and development guide in the next two sections.

## Deployment Guide

The general philosophy is to manage all configurations and deployed notebooks
as code using git processes for version control. It is crucial that the
actual deployed notebooks are versioned controlled to facilitate operational
debugging and troubleshooting.

### Initial Deployment

1. Fork this repo in your organization’s github/gitlab (henceforth
my-context-graph repo) & set the upstream to the original
context-graph-analytics repo ([Fork a repo - GitHub Docs](https://docs.github.com/en/get-started/quickstart/fork-a-repo) ). This
ensures that any configurations and generated files will be in your
organization’s git environment along with the proper access controls. The
my-context-graph repo will be a private repo in your organization.
1. Clone the my-context-graph repo to your laptop or development environment.
1. Create a deployment branch `deploy_v1`.
1. Edit the config files (details TBD) to setup the data sources for the pipeline
1. Run `python3 solacc.py generate` to generate the notebooks (the generated files will reside in the deploy folder.
1. Commit the config files and generated notebooks to the `deploy_v1` branch and push to the my-context-graph repo
1. [The following steps in Databricks can be automated via CLI/API.] Create the git repo integration to my-context-graph repo in your Databricks workspace
1. Navigate to the my-context-graph repo in your Databricks workspace and set the branch to `deploy_v1`
1. Setup the DLT pipeline jobs using generated notebooks in the deploy folder of the my-context-graph repo (`deploy_v1` branch) – This step can be automated via CLI in the future.
1. Create any needed views

### Redeployment after a configuration change

Examples of configuration change include adding/removing a data source or
tweaking the notebook logic. Note that you will manage the config files and
generated notebooks as code using standard git processes.

1. Edit the config files or template files or source code to incorporate the change. (You are free to use a separate branch if you like)
1. Run `python3 solacc.py generate` to generate the notebooks (the generated files will reside in the deploy folder
1. Commit the config files and generated notebooks to the deploy_v1 branch (or you can use a separate branch) and push to the my-context-graph repo
1. [The following steps in Databricks can be automated via CLI/API.]  Navigate to the my-sirens repo in each of your Databricks workspace and “pull” the branch deploy_v1 for the latest changes
1. Restart the DLT pipeline jobs if you are using DLT pipeline jobs in continuous processing mode, so that the latest notebooks will be picked up. Batch or scheduled jobs should pick up the latest notebooks automatically on the next scheduled run.

### Solution accelerator upgrade workflow

There is a chance that after you have deployed the solution accelerator, a new
version of the solution accelerator is released. The following steps walk you
through the upgrade process assuming you have not made substantial changes to
the source code or the template files AND you do want to upgrade to that new
version.

1. Fetch the version of context-graph-analytics you want using the appropriate git commands ([Syncing a fork - GitHub Docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork) ). This will update my-context-graph repo (main branch) with the latest changes.
1. Rebase/merge to the deploy_v1 branch or create a new branch as needed.
1. Repeat the above deployment steps to generate notebooks and deploy to Databricks workspace

## Development Guide

### Running tests

To run the provided integration or smoke tests:

1. Clone the repo (either the original or the clone) to your laptop
2. Change directory to the root directory of the repo
3. Ensure you have installed the required packages using `pip3 install -r requirements.txt`
4. Run `pytest`

To get coverage report:

1. `pip3 install coverage`
2. `coverage run -m pytest`
3. `coverage report -m`

### Test-driven development workflow

This solution accelerator is based on a simple code generation framework that
is controlled by a single configuration file `config/cga.json`. For simple
notebooks, the `id` of a notebook specification corresponds to the jinja2
source template file in the `templates` folder and corresponds to the output
notebook file name. Here are guidelines on how to develop and add a new output
notebook to this solution accelerator following a [test-driven development](https://en.wikipedia.org/wiki/Test-driven_development) paradigm.

1. First develop and test the new feature as a notebook (or DLT notebook) in a Databricks workspace. 
2. Create a new branch and prefix the branch name with your name.
3. Create a new test case in `src/test_everything.py` using the notebook you developed and tested. The provided `smoke01` test will be a good example of how to create a test case.
4. Decide how you would like to generate the code for your notebook. Most
notebooks can be templatized using Jinja2 syntax and generated as a simple
notebook - this is the default branch in the code generation logic in
`src/solacc.py`. In some cases where custom code generation is needed, create a
new code generation function in `src/graph_pipelines.py` and add a branch to
the code generation logic in `src/solacc.py`. 
5. Use the test case you created earlier to drive the development and testing.
6. Ensure that `pytest` passes.
7. Push your branch to the origin and open a pull request.
8. Solicit code reviews for the pull request
9. Once the pull request is approved, ask the contact author to merge the code into main.
10. Optionally update the `test_drive` branch.

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| jinja2 | Templating library | BSD-3 | https://github.com/pallets/jinja/ |
| click  | CLI framework  | BSD-3 | https://github.com/pallets/click |
| jsonschema  | json validation  | MIT | https://github.com/python-jsonschema/jsonschema |
| databricks-cli | databricks cli | Apache 2.0 | https://github.com/databricks/databricks-cli |

## Acknowledgements

This solution accelerator is made possible with the help of:

* M from [actionable.today](https://www.actionable.today/), for his review, comments and discussions.
