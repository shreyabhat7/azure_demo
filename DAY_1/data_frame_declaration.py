# Databricks notebook source
# MAGIC %md
# MAGIC make this structured data

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
import datetime

# COMMAND ----------

dt='''{
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
}'''


# COMMAND ----------

import json
data = json.loads(dt)
rdd = spark.sparkContext.parallelize([data])
df = spark.read.json(rdd)
# df = spark.createDataFrame(data)
df.display()
