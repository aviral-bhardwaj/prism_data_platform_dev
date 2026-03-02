# Databricks notebook source
dbutils.widgets.text('gidwave','')
GidWave = dbutils.widgets.get('gidwave')
dbutils.widgets.text('jobrunid','')
dbutils.widgets.text('username','')
dbutils.widgets.text('jobid','')
jr=dbutils.widgets.get('jobrunid')
un=dbutils.widgets.get('username')
jid=dbutils.widgets.get('jobid')

# COMMAND ----------

if GidWave == '':
  dbutils.notebook.exit('Please provide a value for GidWave')
else:
  result = ",".join(f"'{x}'" for x in GidWave.split(","))

  print(result)

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from pyspark.sql import types as T
from linkage_table import *
from datetime import *
import json

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# Setting up variables
bronze_layer = 'prism_bronze'
silver_layer = 'prism_silver'
gold_layer = 'prism_gold'

raw_answers_schema = 'decipher_raw_answers'
canonical_tables_schema = 'canonical_tables'
decipher_schema = 'decipher'
hotels_schema = 'us_hotels'

fact_response_table = 'st_fact_response'
dim_question_table = 'dim_question'
fact_nps_table = 'fact_nps'
gold_mapping_table = 'gold_mapping'
mod_table = 'mod_loyalty_attributes'

# COMMAND ----------

spark.sql(f"DELETE from {gold_layer}.{hotels_schema}.{mod_table} where gid_wave in ({result})")

# COMMAND ----------

# Reading fact_nps
df_fact_response = spark.sql(f"select * from {gold_layer}.{hotels_schema}.{fact_response_table} where deleted_in_source=False")
df_dim_question = spark.sql(f"select * from {silver_layer}.{canonical_tables_schema}.{dim_question_table}")
df_gold_mapping = spark.sql(f"select * from {bronze_layer}.{decipher_schema}.{gold_mapping_table}")

df_attributes = (
    df_gold_mapping
    .filter((F.col('mapping_category')=='Loyalty Program Attributes'))
    .select('map_field_value_1', 'map_field_value_2')
    .withColumnsRenamed({
        "map_field_value_1": "id_survey_question",
        "map_field_value_2": "Attribute",
        })
    .withColumn('Attribute category', F.lit('Hotels'))
    )

# Creating filtered fact_response
df_fact_final = (
    df_fact_response.drop('dte_create')
    .join(df_dim_question, on=['gid_question'], how='left')
    .where(F.col('desc_question_grp')=='Loyalty Program Attributes')
    .withColumn('Loyalty program code', F.substring(F.col("id_survey_question"), -3, 1))
    .withColumn('Top 2', 
                F.when(F.col('txt_answer').isin(["Completely agree 5", "4"]), F.lit('Top 2 Box'))
                 .when(F.col('txt_answer').isin(["Do not agree at all 1", "2", "3"]), F.lit('Other answer text'))
                 .when(F.col('txt_answer').isin(["Not sure / Not applicable"]), F.lit('Not apply'))
                )
    .withColumn('Top 1', 
                F.when(F.col('txt_answer').isin(["Completely agree 5"]), F.lit('Top 1 Box'))
                 .when(F.col('txt_answer').isin(["Do not agree at all 1", "2", "3", "4"]), F.lit('Other answer text'))
                 .when(F.col('txt_answer').isin(["Not sure / Not applicable"]), F.lit('Not apply'))
                )
    .withColumn('dte_create', F.current_timestamp())
    .join(df_attributes,
          on=['id_survey_question'],
          how='inner')
    .select(
        'gid_respondent',
        'gid_question',
        'gid_provider',
        'gid_wave',
        'txt_answer',
        'num_answer',
        'Loyalty program code',
        'Top 2',
        'Top 1',
        'Attribute',
        'Attribute category',
        'dte_create'
        )
    )


# COMMAND ----------

# Saving table to the Gold Layer
(
    df_fact_final
    .write
    .format('delta')
    .option("delta.columnMapping.mode", "name")
    .mode('overwrite')
    .saveAsTable(f"{gold_layer}.{hotels_schema}.{mod_table}")
)

# COMMAND ----------

target_table=f"{gold_layer}.{hotels_schema}.{mod_table}"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)