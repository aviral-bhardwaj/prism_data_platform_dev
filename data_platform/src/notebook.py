# Databricks notebook source
# MAGIC %md
# MAGIC # Default notebook
# MAGIC
# MAGIC This default notebook is executed using Databricks Workflows as defined in resources/data_plat_demo.job.yml.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from data_plat_demo import main

main.get_taxis(spark).show(10)