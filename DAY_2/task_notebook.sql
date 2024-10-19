-- Databricks notebook source
create schema if not exists dev_catalog.gold

-- COMMAND ----------

select * from test_databricks.default.products

-- COMMAND ----------

select sum(product_price),product_category from  test_databricks.default.products group by product_category

-- COMMAND ----------

create table if not exists dev_catalog.gold.total_price_category as (
select sum(product_price) as total_price, product_category from test_databricks.default.products group by product_category)
