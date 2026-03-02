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
phase = spark.conf.get("phase")

# COMMAND ----------

# Set at Spark session level
spark.conf.set("spark.sql.caseSensitive", "true")

# COMMAND ----------

file_name = 'raw_data'
table_name = 'raw_data'

# COMMAND ----------

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/{phase}/{file_name}/'

# COMMAND ----------

@dlt.view
def raw_response_json_stream():
    df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("multiline", "true")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.partitionColumns", "")
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 .load(f"{json_path}"))  \
                 .withColumnRenamed("QSamp", "qsamp_1")   \
                 .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
    return df

# COMMAND ----------


dlt.create_streaming_table(
    f"{table_name}", comment="Raw data json stream to delta bronze"
    )

# COMMAND ----------

dlt.apply_changes(
        target=f"{table_name}",
        source="raw_response_json_stream",
        keys=["uuid"],  # Assuming 'col' is the correct key based on the error message
        sequence_by="bronze_updated_at",
        stored_as_scd_type=2,
        except_column_list=["bronze_updated_at"],
        track_history_except_column_list = ["bronze_updated_at"]
    )