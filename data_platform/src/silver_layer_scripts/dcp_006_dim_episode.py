# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# DBTITLE 1,importing config file
# Imports
from pyspark.sql import functions as F
#from data_plat_cdc_logic import *
from data_plat_cdc_logic_updated import *

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,defining silver layer table
silver_layer = 'prism_silver.canonical_tables'
# Creating Variables
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_catalog = "canonical_tables"
decipher_catalog = "decipher"

questions_mapping_table = "questions_mapping"
answers_mapping_table = "answers_mapping"
survey_metadata_table = "survey_metadata"
dim_instrument_table = "dim_instrument"
dim_wave_table = "dim_wave"
dim_episode_table = "dim_episode"


# COMMAND ----------

# DBTITLE 1,raw data
# Read answers mapping table from bronze layer, filtering for product name answers
df_answers_mapping = spark.sql(
    f"""
    SELECT survey_id,
           layout_id,
           txt_answer,
           recoded_txt_answer
    FROM {bronze_layer}.{decipher_catalog}.{answers_mapping_table}
    WHERE id_survey_question = 'nam_episode' and txt_answer is not null
    """
)
# Read instrument and wave dimension tables from silver layer
df_instrument = spark.sql(f'Select * from {silver_layer}.{canonical_tables_catalog}.{dim_instrument_table}')
df_wave = spark.sql(f'Select * from {silver_layer}.{canonical_tables_catalog}.{dim_wave_table}')

# Join instrument and wave tables on gid_instrument to get survey_id and layout_id
df_instrument_wave = df_instrument.join(df_wave, 'gid_instrument', how='inner').select('gid_instrument','survey_id','layout_id')
# Join with answers mapping to get provider names
df_answer_long = df_instrument_wave.join(df_answers_mapping, ['survey_id', 'layout_id'], how='inner')
# Select unique provider names and their associated instrument IDs
df_episode_final = (
    df_answer_long
    .withColumnRenamed('txt_answer', 'nam_episode')\
        .withColumnRenamed('recoded_txt_answer', 'nam_episode_disp')\
    .select('gid_instrument', 'nam_episode','nam_episode_disp')
    .dropDuplicates(['gid_instrument', 'nam_episode'])
)

# COMMAND ----------

# DBTITLE 1,CDC Implementation
# If table exists, run CDC logic
if spark.catalog.tableExists(f"{silver_layer}.{canonical_tables_catalog}.{dim_episode_table}"):

    dict_df = {dim_episode_table: df_episode_final}
    key_cols = {dim_episode_table: ['nam_episode','gid_instrument']}

    df_new_records = dataplatform_cdc(tables_dict = dict_df, key_cols = key_cols)

    if df_new_records.isEmpty():
        dbutils.notebook.exit("No new records found")
    else:
        df_new_records=df_new_records.withColumn("jobrunid", F.lit(job_runid))
        df_new_records.select(spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_episode_table}").columns)  \
            .write.format("delta")  \
            .mode("append") \
            .option("mergeSchema", "true")   \
            .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_episode_table}")

# Else save new table
else:
    df_episode_final = (
        df_episode_final
        .withColumn('dte_create', F.current_timestamp())
        .withColumn('dte_update', F.current_timestamp())
        .withColumn('jobrunid', F.lit(job_runid))
        )

    # Adding the unique ID column
    df_episode_final = df_episode_final.withColumn("gid_episode", F.row_number().over(Window.orderBy("nam_episode")))

    df_episode_final \
        .write.format("delta")  \
        .mode("append") \
        .option("mergeSchema", "true")   \
        .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_episode_table}")