<img src="https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png" width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Context Graph Analytics
___
Contact Author: <lipyeow@databricks.com>

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

## Technical Overview

This solution accelerator:
* assumes that the raw data from security appliances (EDR, IAM, Network etc.) is already being collected or exported to bronze tables.
* provides sample DLT pipelines to extract a property graph model in the form of edges or triples into silver tables and further aggregates the silver tables via different time buckets into gold tables.
* provides a notebook to create the unifying set of edge/triple views that are used to query all the edges.
* provides a notebook that demonstrates how Entity resolution packages can be used to produce `same_as` edges and how these `same_as` edges can be used in the graph analytics to resolve and investigate different entities even though the entities are represented differently in different data sets.
* provides a notebook that demonstrates how graph navigation can be used in an investigation, 
* provides a demo UI that demonstrate how the graph traversals can be used in an investigation workflow for analysts with no programming experience. 
* provides ... tbd

___

## Reference Architecture

<img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/Context_Graph_Architecture.png" width="600px">

___

## Quickstart Guide

If you just want to try out the solution accelerator without any additional customization in terms of target database names etc. in a Databricks workspace:

1. Ensure you have git integration setup in the workspace 
2. Clone this repo to the workspace
3. Switch to the `staging_latest` branch. The deploy folder will contain a generic version of the generated notebooks.
4. Run the `bronze_sample.py` notebook to load the sample bronze tables
5. Setup the DLT pipeline job for the edge extraction & aggregation pipelines using the `dlt_edges.sql` notebook.
6. Run the DLT pipeline job.
7. Open up the `investigation.py` notebook and follow the steps to run it.

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

