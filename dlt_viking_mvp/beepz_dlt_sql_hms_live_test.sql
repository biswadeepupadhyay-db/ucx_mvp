-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE employee_bronze
AS SELECT
  *
FROM
  read_files(
    'dbfs:/FileStore/beepz/ingestion/source_data_emp.csv',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  )

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE employee_silver
AS SELECT
  *
FROM LIVE.employee_bronze
where salary > 10000
