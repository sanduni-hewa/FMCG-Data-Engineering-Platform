# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC USE CATALOG fmcg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold; -- Parent company already have cleaned, report ready data in gold.
# MAGIC -- Silver and Bronze layers for transforming data in child company.
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver; 
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP CATALOG fmcg CASCADE;