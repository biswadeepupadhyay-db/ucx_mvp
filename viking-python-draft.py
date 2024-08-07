# Databricks notebook source
dbutils.secrets.list('sa-viking-ucx')

# COMMAND ----------

dbutils.secrets.get(scope="sa-viking-ucx", key="fs_azure_key")

# COMMAND ----------


