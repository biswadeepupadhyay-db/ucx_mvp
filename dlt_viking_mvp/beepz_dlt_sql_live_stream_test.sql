-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE employee_bronze
AS SELECT
  *
FROM
  STREAM read_files(
    'dbfs:/FileStore/beepz/ingestion/',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE employee_silver
AS SELECT
  *
FROM STREAM(LIVE.employee_bronze)
where salary > 10000
