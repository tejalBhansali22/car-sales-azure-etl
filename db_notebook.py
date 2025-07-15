# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE CATALOG 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG cars_catalog CASCADE;
# MAGIC CREATE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SCHEMA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold;