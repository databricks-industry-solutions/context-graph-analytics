# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook sets up the companion cluster(s) to run the solution accelerator with. It also creates the workflow job specification to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are not user-specific. If the workflow and cluster created here are modified, running this script again after modification resets them.
# MAGIC  
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 1,Install required packages
# MAGIC %pip install git+https://github.com/databricks-industry-solutions/notebook-solution-companion git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems 

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion


# COMMAND ----------

# DBTITLE 1,This is currently required by the solacc.companion package to track the DLT pipeline ids etc.
spark.sql(f"CREATE DATABASE IF NOT EXISTS databricks_solacc LOCATION '/databricks_solacc/'")
spark.sql(f"CREATE TABLE IF NOT EXISTS databricks_solacc.dlt (path STRING, pipeline_id STRING, solacc STRING)")
dlt_config_table = "databricks_solacc.dlt"


# COMMAND ----------

# DBTITLE 1,This is the DLT pipeline specification for the DLT task in the Multi-Task Job
pipeline_json = {
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "LEGACY"
            }
        }
    ],
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "edition": "ADVANCED",
    "photon": True,
    "libraries": [
        {
            "notebook": {
                "path": "dlt_edges"
            }
        }
    ],
    "name": "runme_solacc_cga_dlt_edges",
    "storage": "/tmp/solacc_cga",
    "target": "solacc_cga"
}

# COMMAND ----------

# DBTITLE 1,Create the DLT pipeline job
pipeline_id = NotebookSolutionCompanion().deploy_pipeline(pipeline_json, dlt_config_table, spark)

print(f"pipeline id : {pipeline_id}")


# COMMAND ----------

# DBTITLE 1,This is the Multi-Task Job specification for running the notebooks in this solution accelerator
job_json = {
        "name": "runme_solacc_cga_test_drive",
        "email_notifications": {},
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "tags": {
          "usage": "solacc_testing",
          "group": "SEC_solacc_automation"
        },
        "tasks": [
            {
                "task_key": "Load_bronze_sample_data",
                "notebook_task": {
                    "notebook_path": "bronze_sample"
                },
                "job_cluster_key": "solacc_cga_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Extract_edges",
                "depends_on": [
                    {
                        "task_key": "Load_bronze_sample_data"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Run_analytics_01",
                "depends_on": [
                    {
                        "task_key": "Extract_same_as_edges"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "analytics_01_impact"
                },
                "job_cluster_key": "solacc_cga_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Create_views",
                "depends_on": [
                    {
                        "task_key": "Extract_edges"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "create_views"
                },
                "job_cluster_key": "solacc_cga_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            },
            {
                "task_key": "Extract_same_as_edges",
                "depends_on": [
                    {
                        "task_key": "Create_views"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "extract_same_as_edges"
                },
                "job_cluster_key": "solacc_cga_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "solacc_cga_cluster",
                "new_cluster": {
                    "spark_version": "11.0.x-cpu-ml-scala2.12",
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-west-2a",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": {
                      "AWS": "i3.xlarge",
                      "MSA": "Standard_D3_v2",
                      "GCP": "n1-highmem-4"
                    },
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": False,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 4,
                    "custom_tags": {
                      "usage": "solacc_testing",
                      "group": "SEC_solacc_automation"
                    }
                }
            }
        ],
        "format": "MULTI_TASK"
}


# COMMAND ----------

# DBTITLE 1,This actually creates/modifies the MTJ and runs it if the widget is set to True.
dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)


# COMMAND ----------

