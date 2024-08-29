-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE reviews_bronze_v2
AS SELECT
  *
FROM
  STREAM read_files(
    'dbfs:/databricks-datasets/amazon/data20K',
    format => 'parquet',
    header => true,
    mode => 'DROPMALFORMED'
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE reviews_silver_v2
AS SELECT
  *
FROM STREAM(LIVE.reviews_bronze_v2)
where rating > 1.0
