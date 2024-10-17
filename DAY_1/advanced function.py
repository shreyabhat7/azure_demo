# Databricks notebook source
simpleData = ((1,"James", "Sales", 3000), \
    (2,"Michael", "Sales", 4600),  \
    (3,"Robert", "Sales", 4100),   \
    (4,"Maria", "Finance", 3000),  \
    (5,"James", "Sales", 3000),    \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100),\
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (None,None,None,None),\
    (None,None,None,None),\
    (None,None,None,None),\
    (None,"Robert",None,2000),\
     (11,"Jack", None, 4100), \
    (12,"Steve", "Sales", None)  
  )
columns= ["id","employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1=df.dropDuplicates(["id"])

# COMMAND ----------

#dropna : deletes all reocrds were Null is present 
df1.dropna().display()

# COMMAND ----------

df1.dropna("all").display()
#to drop Null values in all columns of same record

# COMMAND ----------

df1.dropna("all",subset="department").display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df2.fillna(value='finance',subset='department').display()

# COMMAND ----------

df2.fillna("finance").display()

# COMMAND ----------

df3=df2.fillna({"salary":4600,"department":"Finance"})

# COMMAND ----------

df3.display()

# COMMAND ----------

df3.replace("Jen","Janet").display()

# COMMAND ----------

help()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
w=Window.partitionBy("department").orderBy(df3.salary.desc())
df3.withColumn("variant",row_number().over(w)).display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
w=Window.partitionBy("department").orderBy(df3.salary.desc())
#col("salary").desc()
df3.withColumn("variant",row_number().over(w)).display()


# COMMAND ----------

df3.withColumn("variant",dense_rank().over(w)).display()

# COMMAND ----------



# COMMAND ----------

df3.withColumn("variant",rank().over(w)).display()

# COMMAND ----------


