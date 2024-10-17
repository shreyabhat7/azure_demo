# Databricks notebook source

print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark is Pre-installed in databricks

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### RDD dataframe - resilient distributed Dataset

# COMMAND ----------

# df function

# .select
# .col to fetch specific column
# .withColumnRenamed to rename
# .withColumnsRenamed multiple column rename
# .withcolumn replace exiting coumn or create new column

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
df=spark.createDataFrame(data)

# COMMAND ----------

df.show() #to view dataframe

# COMMAND ----------

df.display() # to view in better html display only in databricks
# df data (records) is distrubted in multiple cores
#dataframe is immutable

# COMMAND ----------

data_set =[(1,'x'),(2,'y'),(3,'z')]
schema =["id","name"]
df1=spark.createDataFrame(data_set,schema)
df1.display()

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema =["id","name","age"]
df=spark.createDataFrame(data)
df.display()

# COMMAND ----------

data=[(1,'a',20,33),(2,'b',30,44)]
schema ="id int ,name string,age int,dummyvar int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

display(df) #same as df.display()

# COMMAND ----------

#dataframe functions

df.select("*")

# COMMAND ----------


df.select("*").display() #selecting all colums *

# COMMAND ----------

# to select few column

df.select("id","age").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

#rename a column

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnRenamed({"id":"emp_id","name":"emp_name"),"age":"emp_age"}).display()

# COMMAND ----------

df.withColumnRenamed("id":"emp_id","name":"emp_name"),"age":"emp_age").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("emp_hiredate",current_date()).display()

# COMMAND ----------

df.withColumn("dummyvar") #check this to replace existing column
