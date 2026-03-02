# Databricks notebook source
dbutils.widgets.text('jobrunid','')
jobrunid=dbutils.widgets.get('jobrunid')

# COMMAND ----------

bronze_target_table='prism_bronze.chatbot_config.system_prompts'
silver_target_table='prism_silver.chatbot_config.system_prompts'
gold_target_table='prism_gold.chatbot_config.system_prompts'

# COMMAND ----------

import pyspark.sql.functions as F

base_path = "abfss://101-data-plat-bronze-layer@npsprismdplatformdldev.dfs.core.windows.net/chatbot_config/"
pattern = base_path + "*.txt"   

# Read text file (each line becomes a row)
df = spark.read.text(pattern)

# Combine all lines into a single row and single column (preserve line breaks)
single_row_df = df.selectExpr(
    "concat_ws('\n', collect_list(value)) AS system_prompt"
)
file_info = df.selectExpr("_metadata.file_name", "_metadata.file_path").first()

file_path = file_info.file_path
# Extract file name (without extension) from metadata
file_name = df.selectExpr("_metadata.file_name").first()[0]
instrument_name = file_name.split(".")[0].lower()

# Add instrument name column
final_df = single_row_df.withColumn(
    "nam_instrument",
    F.lit(instrument_name)
).withColumn('dte_ucreate', F.current_timestamp()).withColumn('jobrunid',F.lit(jobrunid))

final_df=final_df.select('nam_instrument','system_prompt','dte_ucreate','jobrunid')
# Delete existing record with the same instrument name from the target table
instrument_name_value = final_df.select('nam_instrument').first()[0]
spark.sql(f"DELETE FROM {bronze_target_table} WHERE nam_instrument = '{instrument_name_value}'")

final_df.write.mode('append').saveAsTable(bronze_target_table)


# COMMAND ----------

# DBTITLE 1,Silver ingetion
spark.sql(f"DELETE FROM {silver_target_table} WHERE nam_instrument = '{instrument_name_value}'")

final_df.write.mode('append').saveAsTable(silver_target_table)

# COMMAND ----------

# DBTITLE 1,Gold data Ingestion
spark.sql(f"DELETE FROM {gold_target_table} WHERE nam_instrument = '{instrument_name_value}'")

final_df.write.mode('append').saveAsTable(gold_target_table)

# COMMAND ----------

# DBTITLE 1,File Archival


archival_path = "abfss://101-data-plat-bronze-layer@npsprismdplatformdldev.dfs.core.windows.net/chatbot_config/archival/"

dbutils.fs.mv(file_path, archival_path)
