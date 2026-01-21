# Databricks notebook source
# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS ecommerce CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce
# MAGIC MANAGED LOCATION 'abfss://container_name@storage_account_name.dfs.core.windows.net/ecommerce_catalog
# MAGIC COMMENT 'Ecommerce project catalog in canada central backed by external location'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES FROM ecommerce;
