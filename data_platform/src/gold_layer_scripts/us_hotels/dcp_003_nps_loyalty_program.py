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
fact_nps_table = 'st_fact_nps'
gold_mapping_table = 'gold_mapping'
mod_table = 'mod_loyalty_program'

# COMMAND ----------

spark.sql(f"DELETE from {gold_layer}.{hotels_schema}.{mod_table} where gid_wave in ({result})")

# COMMAND ----------

# Reading fact_nps
df_fact_nps = spark.sql(f" select * from {gold_layer}.{hotels_schema}.{fact_nps_table} where  gid_wave in ({result})")
df_fact_response =spark.sql(f" select * from {gold_layer}.{hotels_schema}.{fact_response_table} where deleted_in_source=False and gid_wave in ({result})" )
df_dim_question = spark.sql(f"select gid_question, id_survey_question,desc_question_grp from {silver_layer}.{canonical_tables_schema}.{dim_question_table} ")


df_fact_nps_final = (
    df_fact_nps.drop('dte_create')
    .filter(F.col('typ_nps').contains("Loyalty NPS"))
    .withColumn('loyalty_program_code', F.regexp_replace('typ_nps', 'Loyalty NPS ', '').cast('integer'))
    )

# Creating filtered fact_response
df_fact_final = (
    df_fact_response.drop('dte_create')
    .select('gid_respondent', 'gid_question', 'txt_answer')
    .join(df_dim_question, on=['gid_question'], how='left')
    .where(F.col('id_survey_question').isin(['Q_LoyaltyProgram_1', 'Q_LoyaltyProgram_2']))
    .withColumn('loyalty_program_code', F.split(F.col('id_survey_question'), '_').getItem(2).cast('integer'))
    )

df_fact_final_not_loyalty = (
    df_fact_response
    .join(df_dim_question, on=['gid_question'], how='left')
    .where((F.col('desc_question_grp').contains('Loyalty Program Member')))
    .groupBy('gid_respondent', 'id_survey_question', 'txt_answer')
    .count()
    .drop('count')
    .withColumn('loyalty_program_membership', 
                F.when(F.col('id_survey_question').contains('99'), F.lit('Do not have membership'))
                .otherwise(F.lit('Have membership'))
                )
    .withColumn('loyalty_program_code', F.regexp_replace('id_survey_question', 'Q_LoyaltyProgram_Memberr', '').cast('integer'))
    )

# Joining both tables
df_fact_join = (
    df_fact_nps_final
    .join(df_fact_final,
          on=['gid_respondent', 'loyalty_program_code'],
          how='inner')
    .withColumn('loyalty_program', F.col('txt_answer'))
    .drop('txt_answer', 'id_survey_question', 'desc_question_grp')
    )

# Creating loyalty program mapping
df_gold_mapping = spark.table(f'{bronze_layer}.{decipher_schema}.{gold_mapping_table}')

df_loyalty = (
    df_gold_mapping
    .filter((F.col('map_field_key_3')=='nam_provider_parent') & (F.col('map_field_key_2')=='Loyalty Program'))
    .select('map_field_value_1', 'map_field_value_2', 'map_field_value_3')
    .withColumnsRenamed({
        "map_field_value_1": "loyalty_program_code",
        "map_field_value_2": "loyalty_program",
        "map_field_value_3": "nam_provider_parent",
        })
    )

# Joining joined table with the loyalty program mapping
df_fact_join_final = (
    df_fact_join
    .join(df_loyalty.drop('loyalty_program_code'),
          on=['loyalty_program'],
          how='inner')
        )

df_fact_join_final_membership_filter = (
    df_fact_final_not_loyalty
    .join(df_loyalty,
          on=['loyalty_program_code'],
          how='inner')
        )

# Final join
df_fact_join_final_membership_join = (
    df_fact_join_final
    .join(df_fact_join_final_membership_filter.drop('loyalty_program_code', 'nam_provider_parent'),  # Add nam_provider_parent here
          on=['gid_respondent', 'loyalty_program'],
          how='inner')
    .drop('txt_answer', 'id_survey_question')
    )

df_fact_final_tier = (
    df_fact_response
    .join(df_dim_question, on=['gid_question'], how='left')
    .where((F.col('desc_question_grp').contains('Loyalty Program Tier')))
    .groupBy('gid_respondent', 'id_survey_question', 'txt_answer')
    .count().drop('count')
    .withColumnRenamed('txt_answer', 'loyalty_program_tier')
    .withColumn('loyalty_program_code', F.regexp_replace('id_survey_question', 'Q_LoyaltyProgram_Tierr', '').cast('integer'))
    .drop('count')
    )

df_fact_join_final_tier_filter = (
    df_fact_final_tier
    .join(df_loyalty,
          on=['loyalty_program_code'],
          how='inner')
    .select('gid_respondent', 'nam_provider_parent', 'loyalty_program', 'loyalty_program_tier')
        )

# Final Join
df_fact_join_final_membership_join_tier = (
    df_fact_join_final_membership_join
    .join(df_fact_join_final_tier_filter.drop('loyalty_program_code'),
          on=['gid_respondent', 'nam_provider_parent', 'loyalty_program'],
          how='inner')
        )

# Final Select
df_fact_join_final_membership_join_tier_select = (
    df_fact_join_final_membership_join_tier
    .withColumn('dte_create', F.current_timestamp())
    .select(
        'gid_respondent',
        'gid_wave',
        'val_nps',
        'val_nps_category',
        'cde_nps_category',
        'loyalty_program_code',
        'loyalty_program',
        'nam_provider_parent',
        'loyalty_program_membership',
        'loyalty_program_tier',
        'dte_create'        
    )
    .withColumnsRenamed(
        {
            "loyalty_program_code": "Loyalty program code",
            "loyalty_program": "Loyalty program",
            "loyalty_program_membership": "Loyalty program membership",
            "loyalty_program_tier": "Loyalty program tier"
        }
    )
    .distinct()
)

# COMMAND ----------

# Saving table to the Gold Layer
(
    df_fact_join_final_membership_join_tier_select
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