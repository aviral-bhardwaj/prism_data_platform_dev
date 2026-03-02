# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Base imports and variables

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Creating variables
# Catalogs
silver_catalog = 'prism_silver'

# Schemas
canonical_tables_schema = 'canonical_tables'
qc_tools_tables_schema = 'qc_tools_tables'

# Tables
# Tables
dim_respondent_table = 'dim_respondent'
fact_response_table = 'fact_response'
dim_question_table = 'dim_question'
dim_instrument_table = 'dim_instrument'
dim_wave_table = 'dim_wave'
qct_input_verbatim_table = 'qct_input_base_verbatim'
adam_aux_input_table = 'qct_adam_aux_input'


# TBD
qct_adam_input_table = 'qct_adam_input'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingesting and cleaning base tables

# COMMAND ----------

# Read input tables
dim_question = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}").select('gid_question', 'id_universal_question', 'id_survey_question')
dim_instrument = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_instrument_table}").select('gid_instrument', 'nam_instrument')
dim_wave = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}").where(F.col('active')==True).select('gid_wave', 'gid_instrument', 'desc_wave')
fact_response = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{fact_response_table}")
df_respondent = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_respondent_table}").select('gid_respondent','id_respondent')
qct_aux_input_table = spark.table(f"{silver_catalog}.{qc_tools_tables_schema}.{adam_aux_input_table}").select('id_universal_question', 'ADAM_universal_question')

# COMMAND ----------

# Filter fact_response to get only current answers
fact_response_clean = fact_response.where(F.col("deleted_in_source") == False)

# COMMAND ----------

# Enrich fact_response with dimensions
print(f"count before joins: {fact_response_clean.count()}")

fact_response_enrich = (
    fact_response_clean
    .join(df_respondent, on=['gid_respondent'], how="left")
    .join(dim_question, on=['gid_question'], how="left")
    .join(dim_wave, on=['gid_instrument', 'gid_wave'], how="inner")
    .join(dim_instrument, on=['gid_instrument'], how="left")
    .join(qct_aux_input_table, on='id_universal_question', how='inner')
    )

print(f"count after joins: {fact_response_enrich.count()}")

# COMMAND ----------

# Selecting pertinent columns
fact_response_enrich_select = (
    fact_response_enrich
    .select(
        'gid_response',
        'gid_respondent',
        'gid_instrument',
        'gid_wave',
        'gid_question',
        'gid_provider',
        'gid_product',
        'gid_episode',
        'gid_channel',
        'gid_initial_channel',
        'gid_final_channel',
        'gid_sub_product',
        'gid_sub_question',
        'id_respondent',
        'num_answer',
        'txt_answer',
        'deleted_in_source',
        'gid_answer_recoded',
        'ADAM_universal_question',        
        'desc_wave', 
        'nam_instrument', 
        )
    )

# COMMAND ----------

# Creating gid_adam as sha2 hash of all columns
fact_response_enrich_select = (
    fact_response_enrich_select
    .withColumn('gid_adam', F.sha2(F.concat_ws('', *fact_response_enrich_select.columns), 256))
    .withColumn('dte_create', F.current_timestamp())
    .withColumn('dte_update', F.current_timestamp())
    )

# COMMAND ----------

# Save table if non exists
if not spark.catalog.tableExists(f"{silver_catalog}.{qc_tools_tables_schema}.{qct_adam_input_table}"):

    # Save table if none exist
    fact_response_enrich_select.write.mode('overwrite').saveAsTable(f"{silver_catalog}.{qc_tools_tables_schema}.{qct_adam_input_table}")

# Delete and append updated data if table already exists
else:
    # Get active wave IDs for the specified instrument
    waves_to_delete = spark.sql(
        f"""
        SELECT w.gid_wave
        FROM {silver_catalog}.{canonical_tables_schema}.dim_wave w
        INNER JOIN {silver_catalog}.{canonical_tables_schema}.dim_instrument i 
        ON w.gid_instrument = i.gid_instrument
        WHERE w.active = 'true'
        """
    )

    if waves_to_delete.count() == 0:
        print(f"No active waves found")
        dbutils.notebook.exit("No active waves found. Exiting notebook.")
    
    else:
        # Convert wave IDs to a comma-separated string for SQL IN clause
        str_waves_to_waves = ', '.join(map(str, list(map(lambda a:a.gid_wave, waves_to_delete.collect()))))
    
        # Delete records from the target table for the selected waves
        spark.sql(
            f"""
            DELETE FROM {silver_catalog}.{qc_tools_tables_schema}.{qct_adam_input_table}
            WHERE gid_wave IN ({str_waves_to_waves})
            """ )

        fact_response_enrich_select.write.mode('append').saveAsTable(f'{silver_catalog}.{qc_tools_tables_schema}.{qct_adam_input_table}')