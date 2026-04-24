# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog
# MAGIC

# COMMAND ----------

 spark
 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold;

# COMMAND ----------

