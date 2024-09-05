# Databricks notebook source
import os
import json
import requests

# COMMAND ----------

dbutils.widgets.text("workspace_url", "https://one-env-beepz-mvp.cloud.databricks.com/", "Databricks Workspace URL.")
dbutils.widgets.text("access_token", "", "Secure Access Token for APIs.")
dbutils.widgets.text("catalog_name", "beepz_viking_stream_test", "Catalog Name")
dbutils.widgets.text("target_schema", "viking_emp_silver", "Target Schema")
dbutils.widgets.text("pipeline_id", "", "DLT Pipeline ID")

# COMMAND ----------

## Define variables and parameterize them
catalog_name = dbutils.widgets.get("catalog_name")
target_schema = dbutils.widgets.get("target_schema")
token = dbutils.widgets.get("access_token")
workspace_url = dbutils.widgets.get("workspace_url")
pipeline_id = dbutils.widgets.get("pipeline_id")
pipeline_type = "stream" 
version = "1"

# COMMAND ----------

## Hitting the DBX API(POST) for creating(cloning) a dlt pipeline on UC
payload = {
    "catalog": catalog_name,
    "target": target_schema,
    "name": f"beepz_pipeline_uc_dlt_{pipeline_type}_{version}",
    "clone_mode": "MIGRATE_TO_UC",
    "configuration": {
        "pipelines.migration.ignoreExplicitPath": "true"
    }
}
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
url = f"{workspace_url}/api/2.0/pipelines/{pipeline_id}/clone"
payload_json = json.dumps(payload)
response = requests.post(url, headers=headers, data=payload_json)

# COMMAND ----------


payload_json = json.dumps(payload)
response = requests.post(url, headers=headers, data=payload_json)

# COMMAND ----------

#print(response.text)
json.loads(response.content.decode('utf-8'))

# COMMAND ----------


