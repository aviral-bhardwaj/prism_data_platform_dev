# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Base imports and variables

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType, IntegerType


# COMMAND ----------

dbutils.widgets.text("waves to be processed", "", "gid_wave(s) (e.g. 123,456)")

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Variables
# Catalogs
silver_catalog = 'prism_silver'
bronze_catalog = 'prism_bronze'


# Catalog and table names
# Schemas
canonical_tables_schema = 'canonical_tables'
qc_tools_tables_schema = 'qc_tools_tables'
ds_models_schema = 'ds_models_outputs'


# Tables
fact_response_table = 'fact_response'
dim_question_table = 'dim_question'
dim_instrument_table = 'dim_instrument'
dim_wave_table = 'dim_wave'
dim_respondent_table = 'dim_respondent'
dim_provider_table = 'dim_provider'
dim_product_table = 'dim_product'
dim_episode_table = 'dim_episode'
dim_channel_table = 'dim_channel'
dim_question_table = 'dim_question'
fact_nps_table = 'fact_nps'
src_translation_table = "qct_translator_mod_output"


# TBD
qct_va_module_input_table = 'qct_va_mod_input'


# COMMAND ----------

# MAGIC %md
# MAGIC #Sources

# COMMAND ----------


active_gid_wave = ','.join([str(row.gid_wave) for row in spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}")
    .filter((F.col("active") == True) )
    .select("gid_wave")
    .collect()])

if dbutils.widgets.get('waves to be processed').strip() != '':
    active_gid_wave = dbutils.widgets.get('waves to be processed')
    print('This is a historical wave Ingestion -> ',active_gid_wave)
else:
    print('This is a current wave Ingestion -> ',active_gid_wave)
    
print('active waves -> ',active_gid_wave)

if len(active_gid_wave) == 0:
    dbutils.notebook.exit("No active gid_wave found.")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Read input tables
# dim_question = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}")
dim_respondent = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_respondent_table}")
dim_instrument = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_instrument_table}")

src_translation = spark.table(f"{bronze_catalog}.{ds_models_schema}.{src_translation_table}").filter(f"gid_wave in ({active_gid_wave})")
fact_nps = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{fact_nps_table}").drop('gid_npsfact')


# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations
# MAGIC

# COMMAND ----------

# Join src_translation with selected columns from dim_respondent on respondent_id, using a left join.
# Drop unnecessary columns after the join.
src_translation_with_respondent = src_translation.join(
    dim_respondent.select('id_respondent', 'termination_code_ed', 'quality_start', 'qualified_status'),
    on=src_translation.respondent_id == dim_respondent.id_respondent,
    how='left'
).drop('id_respondent')

# COMMAND ----------

# Add NPS category and creation timestamp, and rename create_timestamp column
df = src_translation_with_respondent.withColumn(
    "cde_nps_category",
    F.when(F.col("val_nps").between(0, 6), F.lit("Detractor"))
     .when(F.col("val_nps").isin(7, 8), F.lit("Passive"))
     .when(F.col("val_nps").isin(9, 10), F.lit("Promoter"))
     .otherwise(F.lit("Error"))
).withColumn(
    "dte_create",
    F.current_timestamp()
).withColumnRenamed(
    "create_timestamp", "translation_create_timestamp"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Selection

# COMMAND ----------

# Select relevant columns for the final DataFrame to be written to the target table
df_final = df.select(
    'gid_translation',
    'gid_instrument',
    'gid_wave',
    'gid_respondent',
    'gid_provider',
    'gid_product',
    'gid_episode',
    'gid_channel',
    'typ_nps',
    'instrument_name',
    'respondent_id',
    'wave',
    'provider_name',
    'product_name',
    'episode_name',
    'channel_name',
    'raw_verbatim',
    'verbatim_original_language',
    'sentence_part_original_lang',
    'verbatim_translated',
    'sentence_parts_translated',
    'verbatim_id',
    'sentence_part_id',
    'gpt_split_model',
    'translation_model',
    'ployglot_detection',
    'fasttext_detection',
    'gid_question_verbatim',
    'gid_question_nps',
    'id_survey_question_nps',
    'id_survey_question_verbatim',
    'gid_initial_channel',
    'gid_final_channel',
    'val_nps',
    'language_detected',
    'termination_code_ed',
    'quality_start',
    'qualified_status',
    'cde_nps_category',
    'dte_verbatim_create',
    'translation_create_timestamp',
    'dte_create'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestion and Deletion of Old Records

# COMMAND ----------

# Get active wave IDs for the specified instrument
waves_to_delete = df_final.select('gid_wave').distinct()

# Convert wave IDs to a comma-separated string for SQL IN clause
str_waves_to_waves = ', '.join(map(str, list(map(lambda a:a.gid_wave, waves_to_delete.collect()))))

if waves_to_delete.count() == 0:
    print(f"No active waves found ")
    dbutils.notebook.exit("No active waves found. Exiting notebook.")

else:
    print(f"Waves ingestion list  -> {str_waves_to_waves}")

    # Delete records from the target table for the selected waves
    spark.sql(
        f"""
        DELETE FROM {silver_catalog}.{qc_tools_tables_schema}.{qct_va_module_input_table}
        WHERE gid_wave IN ({str_waves_to_waves})
        """ )

    df_final.write.mode('append').saveAsTable(f'{silver_catalog}.{qc_tools_tables_schema}.{qct_va_module_input_table}')