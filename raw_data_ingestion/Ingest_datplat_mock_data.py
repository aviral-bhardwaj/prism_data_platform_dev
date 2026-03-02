# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark import pandas as ps

# COMMAND ----------

fact_df = spark.read.csv('/Volumes/prism_bronze/landing_volumes/landing_files/sharepoint/01 - Cross-Team Collaboration/1 - Projects/01 - Data Platform/03 Instruments/00_ingest_manual_data_files/us_qsr_001/25q1/fact_response.csv', header=True, inferSchema=True)

# fact_df.display()
fact_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'prism_silver.sandbox_dev.fact_response')

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

def convert_void_columns_to_string(df):
    void_cols = []
    
    for field in df.schema.fields:
        col_name = field.name
        # Check if the column contains only nulls
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        if non_null_count == 0:
            void_cols.append(col_name)
    
    # Cast void columns to StringType
    for col_name in void_cols:
        df = df.withColumn(col_name, col(col_name).cast(StringType()))
    
    return df

# Example usage


# COMMAND ----------

dim_dfs_dict = ps.read_excel('/Volumes/prism_bronze/landing_volumes/landing_files/sharepoint/01 - Cross-Team Collaboration/1 - Projects/01 - Data Platform/03 Instruments/00_ingest_manual_data_files/us_qsr_001/25q1/ingestion_taxonomy.xlsx', sheet_name=None, dtype=str)

for t_name, t_df in dim_dfs_dict.items():
    print(t_name, t_df.columns)
    sp_df = t_df.to_spark().toDF(*[c.replace(' ', '_') for c in t_df.columns])
    # sp_df.display()
    spark.sql(f"DROP TABLE IF EXISTS prism_silver.sandbox_dev.{t_name}")
    sp_df = convert_void_columns_to_string(sp_df)
    sp_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f'prism_silver.sandbox_dev.{t_name}')