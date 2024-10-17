# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dt={
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}

data=spark.createDataFrame([dt])

# COMMAND ----------

data.display()

# COMMAND ----------

data.withColumn("toppings",explode("topping"))\
.withColumn("toppings_id",col("toppings.id"))\
.withColumn("toppings_type",col("toppings.type"))\
.drop("topping")\
.drop("toppings")\
.withColumn("batters_list",explode("batters.batter"))\
.drop("batters")\
.withColumn("batters_id",col("batters_list.id"))\
.withColumn("batters_type",col("batters_list.type"))\
.drop("batters_list").display()

