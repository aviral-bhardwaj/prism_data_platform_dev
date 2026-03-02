# Databricks notebook source
import dlt
import json
from datetime import datetime
from pyspark.sql import functions as F

# COMMAND ----------

csv_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_files_endpoint_csv/'

# COMMAND ----------

@dlt.view
def raw_answers_mapping_csv_stream():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("multiLine", "true")           # Crucial for line breaks within fields
        .option("header", "true")
        .option("quote", "\"")                 # Matches pandas/csv.QUOTE_ALL
        .option("escape", "\"")                # Escapes inner quotes if any
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")          # Allows recovery from minor format issues
        .load(f"{csv_path}survey_file_Answers_mapping*.csv")         # Replace with your storage location
        .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
        .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
    )
    return df

@dlt.view
def raw_questions_mapping_csv_stream():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("multiLine", "true")           # Crucial for line breaks within fields
        .option("header", "true")
        .option("quote", "\"")                 # Matches pandas/csv.QUOTE_ALL
        .option("escape", "\"")                # Escapes inner quotes if any
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")          # Allows recovery from minor format issues
        .load(f"{csv_path}survey_file_Questions_mapping*.csv")         # Replace with your storage location
        .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
        .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
    )
    return df

@dlt.view
def raw_variable_mapping_csv_stream():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("multiLine", "true")           # Crucial for line breaks within fields
        .option("header", "true")
        .option("quote", "\"")                 # Matches pandas/csv.QUOTE_ALL
        .option("escape", "\"")                # Escapes inner quotes if any
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")          # Allows recovery from minor format issues
        .load(f"{csv_path}survey_file_Variable_mapping*.csv")         # Replace with your storage location
        .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
        .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
    )
    return df

@dlt.view
def raw_gold_mapping_csv_stream():
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("multiLine", "true")           # Crucial for line breaks within fields
        .option("header", "true")
        .option("quote", "\"")                 # Matches pandas/csv.QUOTE_ALL
        .option("escape", "\"")                # Escapes inner quotes if any
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")          # Allows recovery from minor format issues
        .load(f"{csv_path}survey_file_gold_mapping*.csv")         # Replace with your storage location
        .withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp"))
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
        .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
    )
    return df


# COMMAND ----------

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.answers_mapping", comment="Manual input for Answers Mapping in each survey."
    )

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.questions_mapping", comment="Manual input for Questions Mapping in each survey."
    )

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.variable_mapping", comment="Manual input for Variable Mapping in each survey."
    )

# creating table for valid records
dlt.create_streaming_table(
    "prism_bronze.decipher.gold_mapping", comment="Manual input for Gold Layer in each survey."
    )

# COMMAND ----------

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.answers_mapping",
    source="raw_answers_mapping_csv_stream",
    keys=["id_survey_question", "num_answer", "survey_id", "layout_id"],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.questions_mapping",
    source="raw_questions_mapping_csv_stream",
    keys=["id_survey_question", "survey_id", "layout_id"],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.variable_mapping",
    source="raw_variable_mapping_csv_stream",
    keys=["id_question", "survey_id", "layout_id"],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)

# Final DLT streaming SCD Type 2 load
dlt.apply_changes(
    target="prism_bronze.decipher.gold_mapping",
    source="raw_gold_mapping_csv_stream",
    keys=[
        "nam_instrument", "nam_country", "mapping_category", 
        "map_field_key_1", "map_field_value_1", 
        "map_field_key_2", "map_field_value_2",
        "map_field_key_3", "map_field_value_3"
        ],
    sequence_by="bronze_updated_at",
    stored_as_scd_type=2,
    # except_column_list=["bronze_updated_at","file_name"],
    track_history_except_column_list=["bronze_updated_at", "file_name"]
)