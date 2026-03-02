# Databricks notebook source
print('fetching common functions and variables')

# COMMAND ----------

# MAGIC %run ../../../../utils/common_functions

# COMMAND ----------

gold_schema = 'prism_gold.us_insurance'
silver_schema = 'prism_silver.canonical_tables'
nam_instrument = 'US Insurance'

print("gold_schema : ", gold_schema)
print("silver_schema : ", silver_schema)
print("nam_instrument : ", nam_instrument)

# COMMAND ----------

