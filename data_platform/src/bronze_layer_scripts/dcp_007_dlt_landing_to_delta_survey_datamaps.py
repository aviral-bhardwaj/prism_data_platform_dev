# Databricks notebook source
import dlt
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_metadata_endpoint/'

# COMMAND ----------

@dlt.view
def raw_survey_metadata_json_stream():
    df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "json")
                 .option("multiline", "true")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.partitionColumns", "")
                 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                 .load(f"{json_path}")
                 .select(F.explode(F.col("variables")).alias("var")).select("var.*")
                 .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
                 .withColumn("file_name", F.col("_metadata.file_path"))
                 .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
                 .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
        )
    return df

# COMMAND ----------

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.survey_metadata", comment="Question metadata in each survey. One row per question/survey"
    )

# COMMAND ----------

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.survey_metadata",
    source="raw_survey_metadata_json_stream",
    keys=["label", "survey_id", "layout_id"],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)