# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Base imports and variables

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce

# COMMAND ----------

# Creating variables
# Catalogs
silver_catalog = 'prism_silver'

# Schemas
canonical_tables_schema = 'canonical_tables'
qc_tools_tables_schema = 'qc_tools_tables'

# Tables
fact_response_table = 'fact_response'
fact_nps_table = 'fact_nps'
dim_question_table = 'dim_question'
dim_instrument_table = 'dim_instrument'
dim_wave_table = 'dim_wave'
dim_respondent_table = 'dim_respondent'
dim_provider_table = 'dim_provider'
dim_product_table = 'dim_product'
dim_episode_table = 'dim_episode'
dim_channel_table = 'dim_channel'

# TBD
vs_input_creation_table = 'qct_vs_input'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingesting and cleaning base tables

# COMMAND ----------

# Read input tables
dim_respondent = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_respondent_table}")
dim_provider = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_provider_table}")
dim_product = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_product_table}")
dim_episode = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_episode_table}")
dim_channel = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_channel_table}")
dim_instrument = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_instrument_table}")

dim_question_nps = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}")  \
                    .select("gid_question", "gid_instrument", "id_survey_question") \
                    .withColumnRenamed("gid_question", "gid_question_nps")  \
                    .withColumnRenamed("id_survey_question", "id_survey_question_nps")
dim_question_verbatim = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}")  \
                    .select("gid_question", "gid_instrument", "id_survey_question") \
                    .withColumnRenamed("gid_question", "gid_question_verbatim") \
                    .withColumnRenamed("id_survey_question", "id_survey_question_verbatim")

dim_wave = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}").filter(F.col('active') == "true")

fact_nps = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{fact_nps_table}").drop('gid_npsfact')
dim_question = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}")

# COMMAND ----------

df_translation_module = spark.sql('SELECT * FROM prism_silver.qc_tools_tables.qct_translator_mod_output')   \
                        .groupBy('gid_instrument','gid_wave','gid_respondent','gid_provider','gid_product','gid_episode','gid_channel','typ_nps', 'verbatim_translated').count()    \
                        .select('gid_instrument', 'gid_wave','gid_respondent', 'gid_provider','gid_product', 'gid_episode','gid_channel', 'typ_nps', 'verbatim_translated')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Verbatim Shield Input

# COMMAND ----------

fact_nps_enrich = fact_nps \
    .join(dim_instrument.select("gid_instrument", "nam_instrument"), on='gid_instrument', how="inner") \
    .join(dim_wave.select("gid_instrument", "gid_wave", "desc_wave"), on=['gid_instrument', 'gid_wave'], how="inner") \
    .join(dim_respondent.select("gid_respondent", "id_respondent","quality_start","end_date","sample_vendor"), on=['gid_respondent'], how="left") \
    .join(dim_provider.select("gid_provider", "nam_provider"), on=['gid_provider'], how="left") \
    .join(dim_product.select("gid_product", "nam_product"), on=['gid_product'], how="left") \
    .join(dim_episode.select("gid_episode", "nam_episode"), on=['gid_episode'], how="left") \
    .join(dim_channel.select("gid_channel", "nam_channel"), on=['gid_channel'], how="left") \
    .join(dim_question_nps, on=['gid_question_nps','gid_instrument'], how="left")  \
    .join(dim_question_verbatim, on=['gid_question_verbatim','gid_instrument'], how="left")

# COMMAND ----------

fact_nps_enrich_translated = fact_nps_enrich.join(df_translation_module, 
            on=['gid_instrument', 'gid_wave','gid_respondent', 'gid_provider','gid_product', 'gid_episode','gid_channel', 'typ_nps'], how='left')\
            .withColumn("verbatim_translated", F.when(F.col("verbatim_translated").isNull(), F.col("txt_verbatim"))   \
            .otherwise(F.col("verbatim_translated")))

fact_nps_enrich_translated_final = fact_nps_enrich_translated   \
    .withColumn('gid_vs', F.sha2(F.concat_ws('', *["gid_instrument", "gid_wave", "gid_respondent", "gid_provider", "gid_product", "gid_episode", "gid_channel", "gid_question_nps", "gid_question_verbatim", "typ_nps", "val_nps", "txt_verbatim"]), 256)) \
                                                                .withColumn("dte_create", F.current_timestamp())\
                                                                .withColumn("dte_update", F.current_timestamp())  

# COMMAND ----------

fact_nps_enrich_translated_final = fact_nps_enrich_translated_final.select(
    "gid_vs",
    "gid_instrument",
    "gid_wave",
    "gid_respondent",
    "gid_provider",
    "gid_product",
    "gid_episode",
    "gid_channel",
    "gid_question_nps", 
    "gid_question_verbatim",
    "nam_instrument",
    "sample_vendor",
    "desc_wave",
    "id_respondent",
    "nam_provider",
    "nam_product",
    "nam_episode",
    "nam_channel",
    "id_survey_question_verbatim",
    "id_survey_question_nps",
    "typ_nps",
    "end_date",
    "quality_start",
    "val_nps",
    "txt_verbatim",
    "verbatim_translated",
    "dte_create",
    "dte_update"
)

# COMMAND ----------

# Get active wave IDs for the specified instrument
waves_to_delete = spark.sql(
    f"""
    SELECT w.gid_wave
    FROM {silver_catalog}.{canonical_tables_schema}.dim_wave w
    INNER JOIN {silver_catalog}.{canonical_tables_schema}.dim_instrument i 
    ON w.gid_instrument = i.gid_instrument AND w.active = 'true'
    """
)

# Convert wave IDs to a comma-separated string for SQL IN clause
str_waves_to_waves = ', '.join(map(str, list(map(lambda a:a.gid_wave, waves_to_delete.collect()))))

if waves_to_delete.count() == 0:
    print(f"No active waves found. Exiting notebook")
    dbutils.notebook.exit("No active waves found. Exiting notebook.")

else:
    print(f"Running the code for the following waves : {str_waves_to_waves}")

    # Delete records from the target table for the selected waves
    spark.sql(
        f"""
        DELETE FROM {silver_catalog}.{qc_tools_tables_schema}.{vs_input_creation_table}
        WHERE gid_wave IN ({str_waves_to_waves})
        """ )

    fact_nps_enrich_translated_final.write.mode('append').saveAsTable(f'{silver_catalog}.{qc_tools_tables_schema}.{vs_input_creation_table}')