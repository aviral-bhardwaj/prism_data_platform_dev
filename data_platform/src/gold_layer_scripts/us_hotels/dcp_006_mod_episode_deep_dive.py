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
dim_answer_recoded_table = 'dim_answer_recoded'

fact_nps_table = 'fact_nps'
gold_mapping_table = 'gold_mapping'
mod_table = 'mod_episode_deep_dive'

# COMMAND ----------

spark.sql(f"DELETE from {gold_layer}.{hotels_schema}.{mod_table} where gid_wave in ({result})")

# COMMAND ----------

# Reading pertinent tables
df_fact_response = spark.sql(f"select * from {gold_layer}.{hotels_schema}.{fact_response_table} where deleted_in_source = false and gid_wave in ({result})")
df_dim_question = spark.sql(f" select * from {silver_layer}.{canonical_tables_schema}.{dim_question_table}")
df_dim_answer_recoded = spark.sql(f"select gid_answer_recoded,recoded_num_answer,recoded_txt_answer from {silver_layer}.{canonical_tables_schema}.{dim_answer_recoded_table}")
df_gold_mapping = spark.sql(f" select * from {bronze_layer}.{decipher_schema}.{gold_mapping_table}")


# COMMAND ----------

# Creating filtered fact_response
df_fact_final = (
    df_fact_response.drop('dte_create')
    .join(df_dim_question, on=['gid_question', 'gid_instrument'], how='inner')
    .where((F.col('desc_question_grp').isin(['Episode deep dive'])) & (F.col('gid_instrument')==2) & (~F.col('id_survey_question').contains('990oe')))
    .join(df_dim_answer_recoded, on='gid_answer_recoded', how='left')
    )

# Creating question long table
df_question_long = spark.sql("""
            SELECT 
            id_survey_question, 
            REGEXP_REPLACE(
                REPLACE(
                REPLACE(
                    CASE 
                    WHEN disp_question LIKE '%following statements%' THEN CONCAT(REGEXP_REPLACE(disp_question, 'following statements:', ''), ' "', question_header_row, '" statement?')
                    ELSE disp_question
                    END,
                    '[pipe: Episode_Provider]',
                    'Provider'
                ),
                ', where 1 means "Do not agree at all" and 5 means "Completely agree"',
                ''
                ),
                ' +',
                ' '
            ) AS disp_question_long
            FROM 
            prism_bronze.decipher.questions_mapping qm 
            WHERE desc_question_grp = 'Episode deep dive' AND survey_id = 777777 and id_survey_question NOT LIKE '%990oe%'
               """)

df_question_long_final = (
    df_question_long.join(df_question_long.groupBy('disp_question_long').count(), on='disp_question_long', how='left')
    )

# COMMAND ----------

# Adding question long to table
df_fact_final_join = (
    df_fact_final
    .drop('num_answer', 'txt_answer')
    .join(df_question_long_final, on=['id_survey_question'], how='left')
    .withColumn('Episode answer text sorting', 
                F.when(F.col('count')=='1', F.col('recoded_num_answer'))
                .otherwise(F.regexp_replace(F.col("id_survey_question"), r".*?(\d+)$", r"$1"))
                )
    .withColumn('dte_create', F.current_timestamp())
    .withColumnsRenamed({
        'id_survey_question': 'question_var',
        'recoded_num_answer': 'num_answer',
        'recoded_txt_answer': 'txt_answer'
        })
    .select(
        'gid_respondent',
        'gid_question',
        'gid_provider',
        'gid_episode',
        'question_var',
        'txt_answer',
        'num_answer',
        'disp_question_long',
        'Episode answer text sorting',
        'gid_wave',
        'dte_create'
        )
    )


# COMMAND ----------

# Saving table to the Gold Layer
(
    df_fact_final_join
    .write
    .format('delta')
    .option("delta.columnMapping.mode", "name")
    .mode('append')
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