# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# COMMAND ----------

# MAGIC %run ./variables

# COMMAND ----------

#output table
target_table = 'mod_reasons_offline_vs_online'

# widget
dbutils.widgets.text("waves_to_refresh", "")

# COMMAND ----------

waves_to_refresh = dbutils.widgets.get("waves_to_refresh")
active_instrument_wave = get_active_gid_wave(silver_schema, nam_instrument, waves_to_refresh)

# COMMAND ----------

df_response_current = spark.sql(f'select * from {gold_schema}.st_fact_response').join(active_instrument_wave, ['gid_instrument', 'gid_wave'], 'inner')
df_respondent = spark.sql(f'select * from {silver_schema}.dim_respondent').select('gid_respondent', 'id_respondent')
df_question = spark.sql(f'select * from {silver_schema}.dim_question').select('gid_question','id_survey_question', 'desc_question_grp')
df_answer_recoded = spark.sql(f'select * from prism_silver.canonical_tables.dim_answer_recoded')    \
                        .select('gid_answer_recoded', 'recoded_txt_answer', 'recoded_num_answer')

# COMMAND ----------

df_offline_online_response = df_response_current \
                    .filter(F.col("desc_question_grp").ilike('Preference of offline over online means'))    \
                    .join(df_respondent, ['gid_respondent'], 'left') \
                    .join(df_answer_recoded, ['gid_answer_recoded'], 'left')    \
                    .filter(~F.col('id_survey_question').like('%oe')) \
                    .withColumn('recoded_txt_answer', F.when(F.col('recoded_txt_answer').isNotNull(), F.col('recoded_txt_answer')).otherwise(F.col('txt_answer'))) \
                    .withColumn('recoded_num_answer', F.when(F.col('recoded_num_answer').isNotNull(), F.col('recoded_num_answer')).otherwise(F.col('num_answer')))  \
                    .drop('dte_create')

df_offline_online_response_final = df_offline_online_response \
    .withColumnRenamed('recoded_txt_answer', 'txt_reasons_offline') \
    .withColumnRenamed('recoded_num_answer', 'num_reasons_offline') \
    .withColumn('num_reasons_offline', F.col('num_reasons_offline').cast(DoubleType()).cast(IntegerType())) \
    .withColumnRenamed('id_survey_question', 'question_var')    \
    .withColumn('dte_create', F.current_timestamp())

# COMMAND ----------

df_offline_online_response_final_select = df_offline_online_response_final.select(
    "gid_wave",
    "gid_respondent",
    "gid_provider",
    "gid_product",
    "gid_episode",
    "question_var",
    "desc_question_grp",
    "txt_reasons_offline",
    "num_reasons_offline",
    "dte_create"
)

# COMMAND ----------

# Delete records from the target table for the selected waves
gid_wave_str = ",".join([str(row['gid_wave']) for row in active_instrument_wave.select('gid_wave').collect()])

spark.sql(
    f"""
    DELETE FROM {gold_schema}.{target_table}
    WHERE gid_wave IN ({gid_wave_str})
    """
)

df_offline_online_response_final_select.write.mode('append').format('delta').saveAsTable(f'{gold_schema}.{target_table}')