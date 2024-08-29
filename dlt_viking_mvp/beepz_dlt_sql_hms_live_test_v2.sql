-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE ratings_bronze
AS SELECT
  *
FROM
  read_files(
    'dbfs:/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  )

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE ratings_silver
AS SELECT
  *
FROM LIVE.ratings_bronze
where rating >= 1.0
