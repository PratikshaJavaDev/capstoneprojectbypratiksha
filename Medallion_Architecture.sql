-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Creating Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS bank_catalog;

USE CATALOG bank_catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Creating Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS  bronze;
CREATE SCHEMA IF NOT EXISTS  silver;
CREATE SCHEMA IF NOT EXISTS  gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.customers(
  customer_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  address STRING,
  credit_score INT,
  join_date DATE,
  last_update TIMESTAMP
)

-- COMMAND ----------

DESCRIBE FORMATTED bronze.customers;

-- COMMAND ----------

select * from bronze.customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Branch Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.branches(
  branch_id STRING,
  branch_name STRING,
  location STRING,
  timezone STRING
)

-- COMMAND ----------

DESCRIBE FORMATTED bronze.branches;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transaction Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.transactions(
    transaction_id STRING,
    customer_id STRING,
    branch_id STRING,
    channel STRING,
    transaction_type STRING,
    amount DOUBLE,
    currency STRING,
    timestamp STRING,
    status STRING
)


-- COMMAND ----------

DESCRIBE FORMATTED bronze.transactions;

-- COMMAND ----------

select * from bronze.transactions;

-- COMMAND ----------


