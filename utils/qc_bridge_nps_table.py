# Databricks notebook source
dbutils.widgets.text("typ_nps_text_input", "", "typ_nps")
dbutils.widgets.text("column_gid_text_input", "", "column_gid")

typ_nps_value = dbutils.widgets.get("typ_nps_text_input")
column_gid_value = dbutils.widgets.get("column_gid_text_input")

# COMMAND ----------

spark.sql(f"insert into prism_silver.qc_tools_tables.ref_typ_nps_mapping (typ_nps, column_gid) values ('{typ_nps_value}','{column_gid_value}')")