# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)


# COMMAND ----------


# Creating Variables
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_schema = "canonical_tables"
decipher_schema = "decipher"

gold_mapping_table = "gold_mapping_test"
dim_wave_table = "dim_wave"
dim_gold_mapping_table = "dim_gold_mapping_test"

# COMMAND ----------


csv_path = "/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_files_endpoint_csv/"

# 🔒 Explicit schema (ONLY columns common to all files)
gold_mapping_schema = StructType([

    StructField("nam_instrument", StringType(), True),
    StructField("nam_country", StringType(), True),
    StructField("mapping_category", StringType(), True),

    StructField("map_field_key_1", StringType(), True),
    StructField("map_field_value_1", StringType(), True),
    StructField("map_field_key_2", StringType(), True),
    StructField("map_field_value_2", StringType(), True),
    StructField("map_field_key_3", StringType(), True),
    StructField("map_field_value_3", StringType(), True),
    StructField("map_field_key_4", StringType(), True),
    StructField("map_field_value_4", StringType(), True),
    StructField("map_field_key_5", StringType(), True),
    StructField("map_field_value_5", StringType(), True),
    StructField("map_field_key_6", StringType(), True),
    StructField("map_field_value_6", StringType(), True),
    StructField("map_field_key_7", StringType(), True),
    StructField("map_field_value_7", StringType(), True),
    StructField("map_field_key_8", StringType(), True),
    StructField("map_field_value_8", StringType(), True),
    StructField("map_field_key_9", StringType(), True),
    StructField("map_field_value_9", StringType(), True),
    StructField("map_field_key_10", StringType(), True),
    StructField("map_field_value_10", StringType(), True),
    StructField("map_field_key_11", StringType(), True),
    StructField("map_field_value_11", StringType(), True),
    StructField("map_field_key_12", StringType(), True),
    StructField("map_field_value_12", StringType(), True),
    StructField("map_field_key_13", StringType(), True),
    StructField("map_field_value_13", StringType(), True),
    StructField("map_field_key_14", StringType(), True),
    StructField("map_field_value_14", StringType(), True),
    StructField("map_field_key_15", StringType(), True),
    StructField("map_field_value_15", StringType(), True),

    # CSV timestamp columns
    StructField("__START_AT", TimestampType(), True),
    StructField("__END_AT", TimestampType(), True),
])

#  Auto Loader 
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("multiLine", "true")
        .option("header", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")
        .schema(gold_mapping_schema)             
        .load(f"{csv_path}survey_file_gold_mapping*.csv")

        # derived / metadata columns (NOT from CSV)
        .withColumn("bronze_updated_at", F.current_timestamp())
        .withColumn("file_name", F.col("_metadata.file_path"))
        .withColumn("survey_id", F.element_at(F.split(F.col("file_name"), "_"), -3))
        .withColumn("layout_id", F.element_at(F.split(F.col("file_name"), "_"), -2))
)

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option(
        "checkpointLocation",csv_path
    ) \
    .trigger(once=True) \
    .table(f"{bronze_layer}.{decipher_schema}.{gold_mapping_table}")
