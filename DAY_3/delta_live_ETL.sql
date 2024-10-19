-- Databricks notebook source
-- use apply changes into for SCD-1 and SCD-2 Handling 

-- COMMAND ----------

create or refresh streaming live table sales_bronze 
as select * from cloud_files("/Volumes/dev_catalog/default/dlt_sales","csv",map("cloudFiles.inferColumnTypes","True"));

-- COMMAND ----------

create or refresh streaming live table customers_bronze 
as select * from cloud_files("/Volumes/dev_catalog/default/dlt_customers","csv",map("cloudFiles.inferColumnTypes","True"));

-- COMMAND ----------

create or refresh streaming live table products_bronze 
as select * from cloud_files("/Volumes/dev_catalog/default/dlt_products","csv",map("cloudFiles.inferColumnTypes","True"));

-- COMMAND ----------

create streaming live table sales_silver
(constraint valid_order_id expect(order_id is not null) on violation drop row)
 as select distinct(*) from stream(live.sales_bronze)

-- COMMAND ----------

create streaming live table customers_silver
(constraint valid_order_id expect(order_id is not null) on violation drop row)
 as select distinct(*) from stream(live.customers_bronze)
