# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Transforming layout raw metadata from landing (ADL) to Delta table (DBX-Bronze)

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

if agg_suffix == 0 or agg_suffix == '' or agg_suffix == '0':
    file_name = f'dcp_datamap.json'
    table_name = f'dcp_datamap'
else:
    file_name = f'dcp_datamap_{agg_suffix}.json'
    table_name = f'dcp_datamap_{agg_suffix}'

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'

# COMMAND ----------

@dlt.table(
    name=table_name+'_'+table_post,
    comment="Schema JSON loaded"
)
def load_json_variables():
    # Read the full JSON file
    df = spark.read.option("multiline", "true").json(f"{json_path}{file_name}") \
            .select(F.explode(F.col("variables")).alias("var")).select("var.*") \
            .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))

    return df