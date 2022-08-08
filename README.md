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

* Assume that raw data from security appliances (EDR, IAM, Network etc.) is being collected or exported to bronze tables.
* This solution accelerator provides the sample DLT pipelines to extract a property graph model in the form of edges or triples into silver tables and further aggregates the silver tables via different time buckets into gold tables.
* Entity resolution packages can be used to produce `same_as` edges that can be used in the graph analytics to incorporate entity resolution information.
* A unifying set of edge/triple views are used to query all the edges.
* A demo UI is also provided to demonstrate how the graph traversals can be used in an investigation workflow. 

___

## Reference Architecture

<img src="https://github.com/lipyeowlim/public/raw/main/img/context-graph/Context_Graph_Architecture.png" width="600px">

___

## Quickstart Guide



&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

