# Databricks notebook source
dbutils.secrets.list('sa-viking-ucx')

# COMMAND ----------

dbutils.secrets.get(scope="sa-viking-ucx", key="fs_azure_key")

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/airlines/'))


# COMMAND ----------

display(dbutils.fs.head('/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv'),3)

# COMMAND ----------

df = spark.read.format("parquet").option("header", "true").load("/databricks-datasets/amazon/data20K")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("rating").count().show()

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv")

# COMMAND ----------

display(df.groupBy('rating').count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC     read_files(
# MAGIC     'dbfs:/databricks-datasets/airlines/part-00000',
# MAGIC     format => 'csv',
# MAGIC     header => true,
# MAGIC     sep => ',',
# MAGIC     mode => 'DROPMALFORMED'
# MAGIC   ) LIMIT 10

# COMMAND ----------


