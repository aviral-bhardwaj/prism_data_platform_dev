# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Transforming layout raw metadata from landing (ADL) to Delta table (DBX-Bronze)

# COMMAND ----------

#Creating Widgets
dbutils.widgets.text('instrument_name', '')
dbutils.widgets.text('wave', '')
dbutils.widgets.text('agg_suffix', '')

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

#Collecting widget values
instrument_name = dbutils.widgets.get('instrument_name').lower()
wave = dbutils.widgets.get('wave').lower()
agg_suffix = dbutils.widgets.get('agg_suffix').lower() 

# COMMAND ----------

if agg_suffix == 0 or agg_suffix == '' or agg_suffix == '0':
    file_name = f'dcp_datamap.json'
    table_name = f'dcp_{instrument_name}_{wave}_datamap'
else:
    file_name = f'dcp_datamap_{agg_suffix}.json'
    table_name = f'dcp_{instrument_name}_{wave}_datamap_{agg_suffix}'

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'

with open(f"{json_path}{file_name}", "r") as f:
    json_file = json.load(f)

df = spark.createDataFrame(json_file['variables'])

# COMMAND ----------

# Adding time stamp

df = df.withColumn('bronze_updated_at', F.lit(datetime.now()).cast("timestamp"))

# COMMAND ----------


df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'prism_bronze.{instrument_name}.{table_name}')