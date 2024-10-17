# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="/Volumes/test_databricks/default/raw"

# COMMAND ----------

def add_ingestion(df):
    df1=df.withColumn("ingestion_date",current_timestamp())
    return df1
