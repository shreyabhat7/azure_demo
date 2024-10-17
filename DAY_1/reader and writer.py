# Databricks notebook source
#/Volumes/test_databricks/default/raw

df=spark.read.csv("/Volumes/test_databricks/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####drop table sales

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

df.display()

# COMMAND ----------

df_products=spark.read.json("/Volumes/test_databricks/default/raw/products.json")

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_customers=spark.read.json("/Volumes/test_databricks/default/raw/customers.json")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.write.saveAsTable("customer")

# COMMAND ----------

df.write.saveAsTable("sales")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_joined=df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"],"inner")

# COMMAND ----------

#df_joined=df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"],"inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_order_date=spark.table("order_dates")

# COMMAND ----------

df_prod_info=df_sales.join(df_order_date,df_sales["order_date"]==df_order_date["order_date"],"inner")

# COMMAND ----------

df_prod_info.display()

# COMMAND ----------

df_customers.where("customer_id=2").display()



# COMMAND ----------

from pyspark.sql.functions import *
df_customers.where(col("customer_id")=='2' or col("customer_city")=="New Michaelview").display()

# COMMAND ----------

df_customers.where("customer_id>2 or customer_city='New Michaelview'").display()

# COMMAND ----------

df_customers.filter("customer_id=2").display()

# COMMAND ----------

df_customers.sort("customer_city").display()

# COMMAND ----------

from pyspark.sql.functions import *
df_customers.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined.groupBy(df_customers["customer_id"]).count().orderBy(df_customers["customer_id"]).display()
