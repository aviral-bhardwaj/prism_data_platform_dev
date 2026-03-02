# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Base imports and variables

# COMMAND ----------

!pip install openpyxl==3.1.0

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd

# COMMAND ----------

# Setting path to file
path_to_file = "/Volumes/prism_bronze/landing_volumes/landing_files/reference_file/adam/ADAM Input file for Data Platform.xlsx"
output_table_name = "prism_silver.qc_tools_tables.qct_adam_aux_input"

# COMMAND ----------

# Read data into dataframe
df = spark.createDataFrame(pd.read_excel(path_to_file))

# Renaming columns and other adjustments
df_final = (
    df
    .withColumnsRenamed({
        "Joined column (Input for ADAM)": "ADAM_universal_question",
        "uid category": "uid_category",
        "Universal QID (uid) ": "id_universal_question"
        })
    )

# Writing to schema
df_final.write.mode("overwrite").format("delta").saveAsTable(output_table_name)