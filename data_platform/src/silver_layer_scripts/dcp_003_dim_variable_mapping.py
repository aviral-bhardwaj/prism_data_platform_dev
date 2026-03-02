# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# MAGIC %md
# MAGIC `Imports`

# COMMAND ----------

# Imports
from data_plat_cdc_logic import dataplatform_cdc
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`
# MAGIC

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Variables
# Creating Variables
# silver_layer = 'prism_silver.canonical_tables'
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_catalog = "canonical_tables"
decipher_catalog = "decipher"

questions_mapping_table = "questions_mapping"
answers_mapping_table = "answers_mapping"
survey_metadata_table = "survey_metadata"
dim_instrument_table = "dim_instrument"
dim_wave_table = "dim_wave"
dim_variable_mapping_table = "dim_variable_mapping"


# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

# DBTITLE 1,Sources
df_wave = spark.sql(f"SELECT gid_instrument, survey_id FROM {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} WHERE active = True")
raw_data = spark.sql("SELECT * FROM prism_bronze.decipher.variable_mapping WHERE __END_AT IS NULL")

# COMMAND ----------

# MAGIC %md
# MAGIC `Joining with dim_instrument to get gid_instrument`

# COMMAND ----------

join_df = raw_data.join(
    df_wave, 
    on=['survey_id'], 
    how='inner'
).drop(df_wave.survey_id)


# COMMAND ----------

# MAGIC %md
# MAGIC `Final Selection`

# COMMAND ----------

# DBTITLE 1,Final Selection
final_df = join_df.select(
 'gid_instrument',
 'id_question',
 'provider',
 'product',
 'episode',
 'channel',
 'initial_channel',
 'final_channel'
)

final_df = final_df.drop_duplicates()


# COMMAND ----------

# MAGIC %md
# MAGIC `CDC Implementation`

# COMMAND ----------

# DBTITLE 1,CDC Implementation
# If table exists, run CDC logic
if spark.catalog.tableExists(f"{silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table}"):

    dict_df = {dim_variable_mapping_table: final_df}
    key_cols = {dim_variable_mapping_table: ['gid_instrument','id_question']}

    df_new_records = dataplatform_cdc(tables_dict = dict_df, key_cols = key_cols)

    if df_new_records.isEmpty():
        dbutils.notebook.exit("No new records found")
    else:
        df_new_records=df_new_records.withColumn("jobrunid", F.lit(job_runid))
        df_new_records.select(spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table}").columns)  \
            .write.format("delta")  \
            .mode("append") \
            .option("mergeSchema", "true")   \
            .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table}")

# Else save new table
else:
    df_final = (
        final_df
        .withColumn('dte_create', F.current_timestamp())
        .withColumn('dte_update', F.current_timestamp())
        .withColumn('jobrunid', F.lit(job_runid))
        )

    # Adding the unique ID column
    df_final = df_final.withColumn("gid_variable_mapping", F.row_number().over(Window.orderBy("nam_provider")))

    df_final \
        .write.format("delta")  \
        .mode("append") \
        .option("mergeSchema", "true")   \
        .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC `Table Creation with CDF Enabled`

# COMMAND ----------

# DBTITLE 1,Table Creation with CDF Enabled
# %sql
# -- Create dim_variable_mapping Table in prism_silver.default with Change Data Feed Enabled
# CREATE TABLE IF NOT EXISTS prism_silver.canonical_tables.dim_variable_mapping ( 
#   gid_variable_mapping  INTEGER,
#   gid_instrument INTEGER,
#   id_question STRING,
#   provider STRING,
#   product STRING,
#   episode STRING,
#   channel STRING,
#   initial_channel STRING,
#   final_channel STRING,
#   dte_update TIMESTAMP,
#   dte_create TIMESTAMP )
#   TBLPROPERTIES (
#     delta.enableChangeDataFeed = true
# );