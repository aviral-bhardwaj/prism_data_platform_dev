# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Transforming layout raw data from landing (ADL) to Delta table (DBX-Bronze)

# COMMAND ----------

import dlt
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

# Access pipeline configuration values
table_post = spark.conf.get("table_post")
instrument_name = spark.conf.get("instrument_name")
wave = spark.conf.get("wave")
agg_suffix = spark.conf.get("agg_suffix")

# COMMAND ----------

# Set at Spark session level
spark.conf.set("spark.sql.caseSensitive", "true")

# COMMAND ----------

if agg_suffix == 0 or agg_suffix == '' or agg_suffix == '0':
    file_name = f'dcp_response.json'
    table_name = f'dcp_response_'
else:
    file_name = f'dcp_response_{agg_suffix}.json'
    table_name = f'dcp_response_{agg_suffix}'

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'



# COMMAND ----------

#loading into table using dlt

@dlt.table(
    name=table_name+'_'+table_post,
    comment="Raw JSON loaded"
)
def load_json_to_delta():
    df = spark.read.option("multiline", "true").json(f"{json_path}{file_name}") \
            .withColumnRenamed("QSamp", "qsamp_1") \
            .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))

    return df