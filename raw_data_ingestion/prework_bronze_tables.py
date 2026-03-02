# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

#imports
import pandas as pd
from pyspark.sql import SparkSession
import configparser
from datetime import datetime
import pyspark.sql.functions as F 

# COMMAND ----------

# Loading the config file
config_path = '/Volumes/prism_bronze/landing_volumes/landing_files/config.ini'
config = configparser.ConfigParser()
config.read(config_path)

#getting the values from config file
instrument_name = config['us_qsr']['instrument_name']
phase = config['us_qsr']['phase']
wave = config['us_qsr']['wave']
start_date = config['us_qsr']['start_date']
end_date = config['us_qsr']['end_date']
target_path = config['us_qsr']['target_path']
survey_id = config['us_qsr']['survey_id']
source_file_name = config['us_qsr']['source_file_name']
bronze_schema = config['us_qsr']['bronze_schema']


# COMMAND ----------

# Define the mount path and target schema
mount_path = f'{target_path}/{instrument_name}_{survey_id}/{wave}/{phase}/{source_file_name}'
target_schema = f'{bronze_schema}.{instrument_name}'

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Load the Excel file using pandas
excel_data = pd.ExcelFile(mount_path)

# Iterate over each sheet and create a Delta table
for sheet_name in excel_data.sheet_names:
    # Read the sheet into a pandas DataFrame
    df = excel_data.parse(sheet_name).astype(str).rename(columns=lambda x: x.strip().replace(' ', '_'))

    # Trim trailing whitespace from all string columns
    df = df.applymap(lambda x: x.rstrip() if isinstance(x, str) else x)

    # Convert the pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.withColumn('instrument_name', F.lit(instrument_name)).withColumn('phase', F.lit(phase)).withColumn('wave', F.lit(wave)).withColumn('start_date', F.lit(start_date)).withColumn('end_date', F.lit(end_date))

    # Clean the sheet name to create a valid table name
    table_name = f"{target_schema}.{sheet_name.replace(' ', '_').lower()}"

    # Write the DataFrame to a Delta table
    spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
    print(f"Created Delta table: {table_name}")
