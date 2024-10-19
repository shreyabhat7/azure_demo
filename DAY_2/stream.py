# Databricks notebook source
schema='Id int , Name string , gender String, Comry string , date string'

# COMMAND ----------

# by default streaming happens every 500ms
df=spark.read.schema(schema).csv("/Volumes/test_databricks/default/streams",header=True)

# COMMAND ----------

#avaialbleNow=True
#.trigger(processingTime='10 seconds')
#.trigger(availableNow=True)
#if file has one column less , data will move one column
# extra column in file , will be ignore


# COMMAND ----------

(spark
.readStream
.schema(schema)
.csv("/Volumes/test_databricks/default/streams",header=True)
.writeStream
.option("checkpointLocation","/FileStore/tables/checkpoint")
.trigger(once=True)
.table("dev_catalog.bronze.stream_data"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.stream_data;
# MAGIC
# MAGIC

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation1")
.load("/Volumes/test_databricks/default/streams")
.writeStream
.option("checkpointLocation","/FileStore/tables/checkpoint_autoloader")
.trigger(once=True)
.table("dev_catalog.bronze.autoloaderdata"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev_catalog.bronze.autoloaderdata
