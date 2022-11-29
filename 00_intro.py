# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this notebook at https://github.com/databricks-industry-solutions/context-graph-analytics on the `web-sync` branch. 

# COMMAND ----------

# MAGIC %md <img src="https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png" width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-11.3ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/11.3ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC # Context Graph Analytics
# MAGIC ___
# MAGIC Contact Author: <lipyeow@databricks.com>
# MAGIC 
# MAGIC ## Use Cases
# MAGIC 
# MAGIC Personas: SOC analyts, Incident Responders, Threat Hunters
# MAGIC 
# MAGIC * Impact analysis (blast radius):
# MAGIC     * Given a compromised user account/apps/ip address, what other user accounts, apps, ip addresses etc. are affected across the enterprise?
# MAGIC * Lateral movement investigation
# MAGIC     * How do you correlate multiple log sources to investigate an attacker moving from one system to another?
# MAGIC * Insider threats investigation
# MAGIC     * How do you leverage HR data together with security logs to find suspicious insider threat activity?
# MAGIC * Query across different data sources:
# MAGIC     * How do you leverage entity resolution modules (eg. Zingg, splink) to query over multiple sources without knowing how each entity is represented in each source?
# MAGIC * Comprehensive attack surface management
# MAGIC * Fusion-center style fraud detection
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ## Reference Architecture
# MAGIC 
# MAGIC <img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/Context_Graph_Architecture.png" width="600px">
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC ## Technical Overview
# MAGIC 
# MAGIC This solution accelerator:
# MAGIC * assumes that the raw data from security appliances (EDR, IAM, Network etc.) is already being collected or exported to bronze tables;
# MAGIC * provides a sample collector notebook for polling okta data via okta's REST API for reference, but collection logic is not the focus of this solution accelerator;
# MAGIC * provides sample DLT pipelines to extract a property graph model in the form of edges or triples into silver tables and further aggregates the silver tables via different time buckets into gold tables;
# MAGIC * provides a notebook to create the unifying set of edge/triple views that are used to query all the edges;
# MAGIC * provides a notebook that demonstrates how Entity resolution packages can be used to produce `same_as` edges and how these `same_as` edges can be used in the graph analytics to resolve and investigate different entities even though the entities are represented differently in different data sets;
# MAGIC * provides a notebook that demonstrates how graph navigation can be used in an investigation; 
# MAGIC * provides a demo UI that demonstrate how the graph traversals can be used in an investigation workflow for analysts with no programming experience. 
# MAGIC * ... tbd ... more to come
# MAGIC 
# MAGIC ___
# MAGIC ## Quickstart Guide
# MAGIC 
# MAGIC If you just want to try out the solution accelerator in a Databricks workspace without any additional customization in terms of target database names etc.:
# MAGIC 
# MAGIC 1. Ensure you have git integration setup in the workspace 
# MAGIC 2. Clone this repo to the workspace
# MAGIC 3. Switch to the `test_drive` branch. The deploy folder will contain a generic version of the generated notebooks.
# MAGIC 4. Run the `RUNME.py` notebook.
# MAGIC 5. Feel free to open up the `analytics_01_impact.py` or the `analytics_02_investigation.py` notebook and play with the analytics.
# MAGIC 
# MAGIC If you are curious what does `RUNME.py` do, it basically automates the following manual steps:
# MAGIC 
# MAGIC 1. Run the `bronze_sample.py` notebook to load the sample bronze tables
# MAGIC 2. Setup the DLT pipeline job for the edge extraction & aggregation pipelines using the `dlt_edges.sql` notebook.
# MAGIC 3. Run the DLT pipeline job.
# MAGIC 4. Run the `create_views.py` notebook.
# MAGIC 5. Run the `extract_same_as_edges.py` notebook.
# MAGIC 6. Run the `analytics_01_impact.py`.
# MAGIC 
# MAGIC If you want to customize or further build on the solution accelerator, follow the deployment guide and development guide in the next two sections.
# MAGIC 
# MAGIC ## Deployment Guide
# MAGIC 
# MAGIC The general philosophy is to manage all configurations and deployed notebooks
# MAGIC as code using git processes for version control. It is crucial that the
# MAGIC actual deployed notebooks are versioned controlled to facilitate operational
# MAGIC debugging and troubleshooting.
# MAGIC 
# MAGIC ### Initial Deployment
# MAGIC 
# MAGIC 1. Fork this repo in your organization’s github/gitlab (henceforth
# MAGIC my-context-graph repo) & set the upstream to the original
# MAGIC context-graph-analytics repo ([Fork a repo - GitHub Docs](https://docs.github.com/en/get-started/quickstart/fork-a-repo) ). This
# MAGIC ensures that any configurations and generated files will be in your
# MAGIC organization’s git environment along with the proper access controls. The
# MAGIC my-context-graph repo will be a private repo in your organization.
# MAGIC 1. Clone the my-context-graph repo to your laptop or development environment.
# MAGIC 1. Create a deployment branch `deploy_v1`.
# MAGIC 1. Edit the config files (details TBD) to setup the data sources for the pipeline
# MAGIC 1. Run `python3 solacc.py generate` to generate the notebooks (the generated files will reside in the deploy folder.
# MAGIC 1. Commit the config files and generated notebooks to the `deploy_v1` branch and push to the my-context-graph repo
# MAGIC 1. [The following steps in Databricks can be automated via CLI/API.] Create the git repo integration to my-context-graph repo in your Databricks workspace
# MAGIC 1. Navigate to the my-context-graph repo in your Databricks workspace and set the branch to `deploy_v1`
# MAGIC 1. Setup the DLT pipeline jobs using generated notebooks in the deploy folder of the my-context-graph repo (`deploy_v1` branch) – This step can be automated via CLI in the future.
# MAGIC 1. Create any needed views
# MAGIC 
# MAGIC ### Redeployment after a configuration change
# MAGIC 
# MAGIC Examples of configuration change include adding/removing a data source or
# MAGIC tweaking the notebook logic. Note that you will manage the config files and
# MAGIC generated notebooks as code using standard git processes.
# MAGIC 
# MAGIC 1. Edit the config files or template files or source code to incorporate the change. (You are free to use a separate branch if you like)
# MAGIC 1. Run `python3 solacc.py generate` to generate the notebooks (the generated files will reside in the deploy folder
# MAGIC 1. Commit the config files and generated notebooks to the deploy_v1 branch (or you can use a separate branch) and push to the my-context-graph repo
# MAGIC 1. [The following steps in Databricks can be automated via CLI/API.]  Navigate to the my-sirens repo in each of your Databricks workspace and “pull” the branch deploy_v1 for the latest changes
# MAGIC 1. Restart the DLT pipeline jobs if you are using DLT pipeline jobs in continuous processing mode, so that the latest notebooks will be picked up. Batch or scheduled jobs should pick up the latest notebooks automatically on the next scheduled run.
# MAGIC 
# MAGIC ### Solution accelerator upgrade workflow
# MAGIC 
# MAGIC There is a chance that after you have deployed the solution accelerator, a new
# MAGIC version of the solution accelerator is released. The following steps walk you
# MAGIC through the upgrade process assuming you have not made substantial changes to
# MAGIC the source code or the template files AND you do want to upgrade to that new
# MAGIC version.
# MAGIC 
# MAGIC 1. Fetch the version of context-graph-analytics you want using the appropriate git commands ([Syncing a fork - GitHub Docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork) ). This will update my-context-graph repo (main branch) with the latest changes.
# MAGIC 1. Rebase/merge to the deploy_v1 branch or create a new branch as needed.
# MAGIC 1. Repeat the above deployment steps to generate notebooks and deploy to Databricks workspace
# MAGIC 
# MAGIC ## Development Guide
# MAGIC 
# MAGIC ### Running tests
# MAGIC 
# MAGIC To run the provided integration or smoke tests:
# MAGIC 
# MAGIC 1. Clone the repo (either the original or the clone) to your laptop
# MAGIC 2. Change directory to the root directory of the repo
# MAGIC 3. Ensure you have installed the required packages using `pip3 install -r requirements.txt`
# MAGIC 4. Run `pytest`
# MAGIC 
# MAGIC To get coverage report:
# MAGIC 
# MAGIC 1. `pip3 install coverage`
# MAGIC 2. `coverage run -m pytest`
# MAGIC 3. `coverage report -m`
# MAGIC 
# MAGIC ### Test-driven development workflow
# MAGIC 
# MAGIC This solution accelerator is based on a simple code generation framework that
# MAGIC is controlled by a single configuration file `config/cga.json`. For simple
# MAGIC notebooks, the `id` of a notebook specification corresponds to the jinja2
# MAGIC source template file in the `templates` folder and corresponds to the output
# MAGIC notebook file name. Here are guidelines on how to develop and add a new output
# MAGIC notebook to this solution accelerator following a [test-driven development](https://en.wikipedia.org/wiki/Test-driven_development) paradigm.
# MAGIC 
# MAGIC 1. First develop and test the new feature as a notebook (or DLT notebook) in a Databricks workspace. 
# MAGIC 2. Create a new branch and prefix the branch name with your name.
# MAGIC 3. Create a new test case in `src/test_everything.py` using the notebook you developed and tested. The provided `smoke01` test will be a good example of how to create a test case.
# MAGIC 4. Decide how you would like to generate the code for your notebook. Most
# MAGIC notebooks can be templatized using Jinja2 syntax and generated as a simple
# MAGIC notebook - this is the default branch in the code generation logic in
# MAGIC `src/solacc.py`. In some cases where custom code generation is needed, create a
# MAGIC new code generation function in `src/graph_pipelines.py` and add a branch to
# MAGIC the code generation logic in `src/solacc.py`. 
# MAGIC 5. Use the test case you created earlier to drive the development and testing.
# MAGIC 6. Ensure that `pytest` passes.
# MAGIC 7. Push your branch to the origin and open a pull request.
# MAGIC 8. Solicit code reviews for the pull request
# MAGIC 9. Once the pull request is approved, ask the contact author to merge the code into main.
# MAGIC 10. Optionally update the `test_drive` branch.
# MAGIC 
# MAGIC ___
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | jinja2 | Templating library | BSD-3 | https://github.com/pallets/jinja/ |
# MAGIC | click  | CLI framework  | BSD-3 | https://github.com/pallets/click |
# MAGIC | jsonschema  | json validation  | MIT | https://github.com/python-jsonschema/jsonschema |
# MAGIC | databricks-cli | databricks cli | Apache 2.0 | https://github.com/databricks/databricks-cli |
# MAGIC 
# MAGIC ## Acknowledgements
# MAGIC 
# MAGIC This solution accelerator is made possible with the help of:
# MAGIC 
# MAGIC * M from [actionable.today](https://www.actionable.today/), for his review, comments and discussions.
