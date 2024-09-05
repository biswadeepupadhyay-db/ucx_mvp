-- Databricks notebook source
select current_metastore();

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

select current_schema();

-- COMMAND ----------

select * from beepz_stream_stage_v2.reviews_stage_v2.reviews_silver_v2;

-- COMMAND ----------



-- COMMAND ----------

ALTER TABLE hive_metastore.reviews_stage_v2.reviews_bronze_v2 UNSET TBLPROPERTIES ('upgraded_from','upgraded_to','upgraded_from_workspace_id')

-- COMMAND ----------

DROP CATALOG beepz_stream_stage_v2 CASCADE

-- COMMAND ----------

truncate table hive_metastore.ucx.permissions

-- COMMAND ----------


