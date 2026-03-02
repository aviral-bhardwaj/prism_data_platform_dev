# Databricks notebook source
import dlt
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/all_surveys_endpoint/'

# COMMAND ----------

@dlt.view
def raw_all_surveys_json_stream():
    df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("multiline", "true")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.partitionColumns", "")
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 .load(f"{json_path}")
                 .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
                 .withColumn("file_name", F.col("_metadata.file_path"))
        )
    return df

# COMMAND ----------

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.all_surveys", comment="Metadata for each survey. One row per survey"
    )

# COMMAND ----------

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.all_surveys",
    source="raw_all_surveys_json_stream",
    keys=["path"],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)