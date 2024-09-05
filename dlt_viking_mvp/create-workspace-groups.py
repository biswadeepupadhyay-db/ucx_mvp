# Databricks notebook source
import os
import json
import requests

# COMMAND ----------

dbutils.widgets.text("workspace_url", "https://one-env-beepz-mvp.cloud.databricks.com/", "Databricks Workspace URL.")
dbutils.widgets.text("access_token", "", "Secure Access Token for APIs.")

# COMMAND ----------

host = dbutils.widgets.get("workspace_url")
token = dbutils.widgets.get("access_token")
gr_creation_endpoint = '/api/2.0/preview/scim/v2/Groups'
url = os.path.join(host, gr_creation_endpoint.lstrip('/'))

# COMMAND ----------

payload1 = {
    "schemas": [
    "urn:ietf:params:scim:schemas:core:2.0:Group"
  ],
    "displayName" : "beepz_group_live"
}

payload2 = {
    "schemas": [
    "urn:ietf:params:scim:schemas:core:2.0:Group"
  ],
    "displayName" : "beepz_group_stream"
}

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# COMMAND ----------


payload_json = json.dumps(payload1)
response = requests.post(url, headers=headers, data=payload_json)

# COMMAND ----------

json.loads(response.content.decode('utf-8'))

# COMMAND ----------


