# Databricks notebook source
#%run /Workspace/Users/bhatshreya98@gmail.com/DAY_1/includes


# COMMAND ----------

# MAGIC %run /Workspace/Users/bhatshreya98@gmail.com/azure_demo/DAY_1/includes

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df_customers=spark.read.json(f"{input_path}/customers.json")

# COMMAND ----------

df_customers.write.mode("overwrite").saveAsTable("customers")

# COMMAND ----------

df_orders=spark.read.csv(f"{input_path}/order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

df_orders.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists products as
# MAGIC select * from json.`/Volumes/test_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`/Volumes/test_databricks/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Create table if not exists products as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/test_databricks/default/raw/products.json`

# COMMAND ----------

dbutils.widgets.text("environment","dev")

# COMMAND ----------

v=dbutils.widgets.get("environment")


# COMMAND ----------


 
df_sales.withColumn("env", lit(v)).display()
 
