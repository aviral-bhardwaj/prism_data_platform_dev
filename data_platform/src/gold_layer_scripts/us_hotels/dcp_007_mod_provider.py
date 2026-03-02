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

from pyspark.sql import functions as F
from linkage_table import *

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

output_schema_name = 'prism_gold'
output_table_name = 'mod_provider'

instrument = 'US Hotels'

# COMMAND ----------

spark.sql(f"delete from prism_gold.us_hotels.mod_provider where gid_wave in ({result})")

# COMMAND ----------

df_instrument = spark.sql(f"select * from prism_silver.canonical_tables.dim_instrument where nam_instrument='{instrument}'")
df_wave = spark.sql(f"select * from prism_silver.canonical_tables.dim_wave where gid_wave in ({result})").join(df_instrument, 'gid_instrument', 'inner').select('gid_instrument','gid_wave','desc_wave')

df_provider = spark.sql('select * from prism_silver.canonical_tables.dim_provider') \
                .join(df_instrument, 'gid_instrument', 'inner') \
                .select('gid_provider','nam_provider')

df_gold_mapping = spark.sql('select * from prism_bronze.decipher.gold_mapping')


df_provider_mapping = df_gold_mapping.filter(F.col('mapping_category')=='Provider') \
    .withColumnRenamed('map_field_value_1', 'nam_provider')    \
    .withColumnRenamed('map_field_value_2', 'nam_provider_parent')  \
    .withColumnRenamed('map_field_value_3', 'provider_tier')  \
    .withColumnRenamed('map_field_value_4', 'provider_segment')  \
    .select('nam_provider', 'nam_provider_parent','provider_tier','provider_segment')

df_provider = df_provider.join(df_provider_mapping, 'nam_provider', 'left')
df_question = spark.sql('select * from prism_silver.canonical_tables.dim_question').select('gid_question','id_survey_question','desc_question_grp', 'id_universal_question')

df_nps = spark.sql('select * from prism_gold.us_hotels.st_fact_nps').join(df_wave, ['gid_instrument', 'gid_wave'], 'inner')
stg_response = spark.sql('select * from prism_gold.us_hotels.st_fact_response')    \
                .join(df_wave, ['gid_instrument', 'gid_wave'], 'inner') \
                .filter(F.col('deleted_in_source')=='False')

# COMMAND ----------

# DBTITLE 1,provider_segment
df_nps_provider_1 = df_nps.filter(F.col('typ_nps')=='Provider NPS')   \
                        .join(df_provider.select('gid_provider','nam_provider','nam_provider_parent','provider_segment'), on='gid_provider', how='left')

# COMMAND ----------

# DBTITLE 1,last_stay_purpose
df_last_stay_response = stg_response.join(df_question, on='gid_question', how='left') \
                                    .filter((F.col('desc_question_grp')=='Last stay purpose') & (~F.col('id_survey_question').ilike('%990')))   \
                                    .withColumnRenamed('txt_answer', 'last_stay_purpose')   \
                                    .select('gid_respondent','gid_provider','last_stay_purpose')

df_nps_provider_2 = df_nps_provider_1.join(df_last_stay_response, on=['gid_respondent','gid_provider'], how='left')

# COMMAND ----------

# DBTITLE 1,Loyalty Program & Membership
df_loyalty_response = stg_response.join(df_question, on='gid_question', how='left') \
    .filter(F.col('desc_question_grp') == 'Loyalty Program Member') \
    .select('gid_respondent', 'txt_answer', 'id_survey_question')

df_loyalty_response = df_loyalty_response.withColumn(
    'loyalty_program_membership',
    F.when(
        (F.col('id_survey_question').contains('999')) | (F.col('id_survey_question').contains('990')),
        F.lit('Do not have membership')
    ).otherwise(F.lit('Having a membership'))
)

df_loyalty_mapping = df_gold_mapping.filter(F.col('mapping_category') == 'Loyalty Program Mapping') \
    .withColumnRenamed('map_field_value_1', 'mapped_id_survey_question') \
    .withColumnRenamed('map_field_value_3', 'loyalty_program') \
    .withColumnRenamed('map_field_value_4', 'nam_provider_parent') \
    .select('mapped_id_survey_question', 'loyalty_program', 'nam_provider_parent')

df_loyalty_response = df_loyalty_response.join(df_loyalty_mapping, df_loyalty_response['id_survey_question'] == df_loyalty_mapping['mapped_id_survey_question'], how='left')

df_nps_provider_3 = df_nps_provider_2.join(
    df_loyalty_response.select('gid_respondent', 'nam_provider_parent', 'loyalty_program', 'loyalty_program_membership'),
    on=['gid_respondent', 'nam_provider_parent'],
    how='left'
)

df_nps_provider_3 = df_nps_provider_3.withColumn(
    'loyalty_program',
    F.when(F.col('loyalty_program').isNull(), F.lit('Not a member of any hotel loyalty program')).otherwise(F.col('loyalty_program'))
).withColumn(
    'loyalty_program_membership',
    F.when(F.col('loyalty_program_membership').isNull(), F.lit('Do not have membership')).otherwise(F.col('loyalty_program_membership'))
)

# COMMAND ----------

# DBTITLE 1,loyalty tier
df_loyalty_tier = stg_response.join(df_question, on='gid_question', how='left') \
    .filter(F.col('desc_question_grp') == 'Loyalty Program Tier') \
    .select('gid_respondent', 'txt_answer', 'id_survey_question')

df_loyalty_tier = df_loyalty_tier.withColumnRenamed('txt_answer', 'loyalty_program_tier')

df_loyalty_tier_mapping = df_gold_mapping.filter(F.col('mapping_category') == 'Loyalty Program Tier Mapping') \
    .withColumnRenamed('map_field_value_1', 'loyalty_program_tier_question') \
    .withColumnRenamed('map_field_value_3', 'loyalty_program') \
    .withColumnRenamed('map_field_value_4', 'nam_provider_parent') \
    .select('loyalty_program_tier_question', 'loyalty_program', 'nam_provider_parent')

df_loyalty_tier = df_loyalty_tier.join(
    df_loyalty_tier_mapping,
    df_loyalty_tier['id_survey_question'] == df_loyalty_tier_mapping['loyalty_program_tier_question'],
    how='left'
)

df_nps_provider_4 = df_nps_provider_3.join(
    df_loyalty_tier.select('gid_respondent', 'nam_provider_parent', 'loyalty_program', 'loyalty_program_tier'),
    on=['gid_respondent', 'nam_provider_parent', 'loyalty_program'],
    how='left'
)

df_nps_provider_4 = df_nps_provider_4.withColumn(
    'loyalty_program_tier',
    F.when(F.col('loyalty_program_tier').isNull(), F.lit('Not applicable')).otherwise(F.col('loyalty_program_tier'))
)

# COMMAND ----------

# DBTITLE 1,Co-Branded Card & Membership
df_cobranded_card = stg_response.join(df_question, on='gid_question', how='left') \
    .filter(F.col('desc_question_grp') == 'CoBranded Credit Card') \
    .select('gid_respondent', 'txt_answer', 'id_survey_question')

df_cobranded_card = df_cobranded_card.withColumn(
    'cobranded_credit_card_membership',
    F.lit('Have membership')
)

df_cobranded_card_mapping = df_gold_mapping.filter(F.col('mapping_category').isin('CoBranded Credit Card  Mapping', 'CoBranded Credit Card Mapping')) \
    .withColumnRenamed('map_field_value_1', 'cobranded_credit_card_mapping_question') \
    .withColumnRenamed('map_field_value_3', 'cobranded_credit_card') \
    .withColumnRenamed('map_field_value_4', 'nam_provider_parent') \
    .select('cobranded_credit_card_mapping_question', 'cobranded_credit_card', 'nam_provider_parent')

df_cobranded_card = df_cobranded_card.join(
    df_cobranded_card_mapping,
    df_cobranded_card['id_survey_question'] == df_cobranded_card_mapping['cobranded_credit_card_mapping_question'],
    how='left'
)

df_nps_provider_5 = df_nps_provider_4.join(
    df_cobranded_card.select('gid_respondent', 'nam_provider_parent', 'cobranded_credit_card', 'cobranded_credit_card_membership'),
    on=['gid_respondent', 'nam_provider_parent'],
    how='left'
)

df_nps_provider_5 = df_nps_provider_5.withColumn(
    'cobranded_credit_card',
    F.when(F.col('cobranded_credit_card').isNull(), F.lit("No, I don't have any hotel loyalty program credit cards")).otherwise(F.col('cobranded_credit_card'))
).withColumn(
    'cobranded_credit_card_membership',
    F.when(F.col('cobranded_credit_card_membership').isNull(), F.lit('Do not have membership')).otherwise(F.col('cobranded_credit_card_membership'))
)

df_nps_provider_5 = df_nps_provider_5.withColumn('dte_create', F.current_timestamp())

# COMMAND ----------

df_nps_provider_final = df_nps_provider_5.select(
    'gid_respondent',
    'gid_provider',
    'gid_wave',
    'val_nps',
    'val_nps_category',
    'cde_nps_category',
    'nam_provider',
    'nam_provider_parent',
    'last_stay_purpose',
    'loyalty_program',
    'loyalty_program_membership',
    'loyalty_program_tier',
    'cobranded_credit_card',
    'cobranded_credit_card_membership',
    'provider_segment',
    'dte_create'
)


# COMMAND ----------

df_nps_provider_final = df_nps_provider_final.withColumn(
    "loyalty_program_membership",
    F.when(
        F.col("loyalty_program_membership") == "Having a membership",
        "Have membership"
    ).otherwise(F.col("loyalty_program_membership"))
)


# COMMAND ----------

df_nps_provider_final.write.mode('append').format('delta').saveAsTable('prism_gold.us_hotels.mod_provider')

# COMMAND ----------

target_table="prism_gold.us_hotels.mod_provider"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)