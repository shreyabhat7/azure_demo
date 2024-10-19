-- Databricks notebook source
use catalog dev_catalog;
--by default data is stored in mentioned catalog directl

-- COMMAND ----------

--create schema bronze;
create schema if not exists dev_catalog.bronze

-- COMMAND ----------

use schema bronze;

-- COMMAND ----------

create table emp(id int, name string, age int)

-- COMMAND ----------

--managed table : databricks maanges inernally the storage without  declaration

-- COMMAND ----------

insert into emp values(1,'a',30);

-- COMMAND ----------

insert into emp values(2,'b',50);
insert into emp values(3,'c',40);
insert into emp values(4,'d',10),(5,'e',100),(6,'f',90);

-- COMMAND ----------

describe detail dev_catalog.bronze.emp;

-- COMMAND ----------

describe extended dev_catalog.bronze.emp;
--detail info of table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC view : virtual table 
-- MAGIC standard : persisted view (SQL)
-- MAGIC temp view : view only for spark session without saving (SQl , pyspark)
-- MAGIC global  : temporay view that can be used in differnet session within same compute(SQL,pyspark)

-- COMMAND ----------

create view emp3 as select * from emp where id >3

-- COMMAND ----------

create temp view emp2 as select * from emp where id >2

-- COMMAND ----------

select * from emp2

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.csv('/Volumes/test_databricks/default/raw/sales.csv',header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("sales_temp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("sales_global_temp")

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from global_temp.sales_global_temp

-- COMMAND ----------

--managed table : handled by datalakehouse , not specifying location
--when we drop all data is gone , cannot be retrieved
--external : need to create external storage and mount it, we have to specify the locaction
--when we drop table is gone , metadata is present

--external table can be used anywhere other than databricks
-- managed can be used only within dtaabrikx

-- COMMAND ----------

-- managed table
--create table x (id int) loction "path"

-- COMMAND ----------

--df.write.option("path","adls").saveAsTable("tablemame")

-- COMMAND ----------

describe history emp

-- COMMAND ----------

select * from emp version as of 2

-- select * from emp @v2 
-- if i want to save this version , perform ctas

-- COMMAND ----------

select * from emp timestamp as of '2024-10-18T04:21:06.000+00:00'

-- COMMAND ----------

select * from emp version as of 4;
-- when we deleted records and want to access and retrieve data
--restore table emp to version as of 4;

-- COMMAND ----------

-- to reduce parquet files for huge data , we can optimze and zorder them deafault zise 128mb
-- optimize tablename;
-- optimze tablename zorder by (columnname,columnname);
-- when the tiny files are still present and stale , storage is still present so it needs to be removed
--vacuum tablename dry run; -- interactive check before delete : itwill show all parquet files that will be deleted
--vacuum tablename;
-- retention period for vaccum file is 7 days or 168 hours
-- to increase the rention period for vaccum
--vacuum tablename retain 0 hours and set spark config value to false

-- COMMAND ----------



-- COMMAND ----------

select * from test_databricks.default.customers

-- COMMAND ----------

describe history test_databricks.default.customers

-- COMMAND ----------

update test_databricks.default.customer set customer_name='Shreya Bhat' where customer_id=3

-- COMMAND ----------

update test_databricks.default.customers set customer_email='Shreya@gmail.com' where customer_id=3

-- COMMAND ----------

delete from test_databricks.default.customers where customer_id=5

-- COMMAND ----------

--drop table test_databricks.default.customers

-- COMMAND ----------

restore table test_databricks.default.customers to version as of 3

-- COMMAND ----------



-- COMMAND ----------

VACUUM test_databricks.default.customers;

-- COMMAND ----------

create or replace function sale_announcement(item_name string , item_price int)
returns string
return concat("The ",item_name ,"is on sale fo $",round(item_price * 0.8,0));

-- COMMAND ----------

select * , sale_announcement(product_id,total_amount) as discount from test_databricks.default.sales

-- COMMAND ----------

create or replace function sale_announcement_check(item_name string )
returns string
return CASE
when item_name ="abc" Then "nnn"
when item_name ="abc" Then "nnn"
else concat("bhjk")
end;


