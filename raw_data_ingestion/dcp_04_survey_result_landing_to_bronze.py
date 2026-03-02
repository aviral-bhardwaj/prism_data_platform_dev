# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Transforming layout raw data from landing (ADL) to Delta table (DBX-Bronze)

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

# Set at Spark session level
spark.conf.set("spark.sql.caseSensitive", "true")

# COMMAND ----------

if agg_suffix == 0 or agg_suffix == '' or agg_suffix == '0':
    file_name = f'dcp_response.json'
    table_name = f'dcp_{instrument_name}_{wave}_response'
else:
    file_name = f'dcp_response_{agg_suffix}.json'
    table_name = f'dcp_{instrument_name}_{wave}_response_{agg_suffix}'

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'

with open(f"{json_path}{file_name}", "r") as f:
    json_file = json.load(f)

df = spark.createDataFrame(json_file)

# COMMAND ----------

df_qsamp_corr = df.withColumnRenamed('QSamp', 'qsamp_1') \
    .withColumn('bronze_updated_at', F.lit(datetime.now()).cast("timestamp"))

# df_qsamp_corr.display()

# COMMAND ----------

df_qsamp_corr.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'prism_bronze.{instrument_name}.{table_name}')