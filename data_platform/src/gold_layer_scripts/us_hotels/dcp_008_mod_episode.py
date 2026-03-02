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

# MAGIC %md
# MAGIC ### Importing Dependencies

# COMMAND ----------

from pyspark.sql import functions as F
from linkage_table import *

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`
# MAGIC

# COMMAND ----------

bronze_layer = 'prism_bronze.decipher'
silver_layer = 'prism_silver.canonical_tables'
gold_layer = 'prism_gold.us_hotels'
output_table_name = 'mod_episode'
nam_instrument = 'US Hotels'
instrument_gid = 2

# COMMAND ----------

spark.sql(f"delete from {gold_layer}.{output_table_name} where gid_wave in ({result})")

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

df_instrument = spark.table(f"{silver_layer}.dim_instrument").filter(
    F.col("nam_instrument") == nam_instrument
)
gid_intrument = df_instrument.select('gid_instrument').collect()[0]['gid_instrument']

df_wave = spark.table(f"{silver_layer}.dim_wave").filter(
    (F.col("gid_instrument") == gid_intrument) & F.expr(f"gid_wave in ({result})")
)
latest_wave_dte = df_wave.agg(F.max('wave_dte_start').alias('wave_dte_start')).collect()[0]['wave_dte_start']

gid_wave = df_wave.filter(F.col("wave_dte_start") == latest_wave_dte).select('gid_wave').collect()[0]['gid_wave']


## dim_respondent
df_respondent = spark.table(f'{silver_layer}.dim_respondent') \
    .filter((F.col('gid_instrument') == instrument_gid) & F.expr(f"gid_wave in ({result})")) \
    .drop('jobrunid')

## dim_episode
df_episode = spark.table(f'{silver_layer}.dim_episode') \
    .filter(F.col('gid_instrument') == instrument_gid).drop('jobrunid')

## stg_fact_nps
df_nps = spark.table(f'{gold_layer}.st_fact_nps').filter((F.col('typ_nps')=='Episode NPS') & (F.col('gid_instrument') == instrument_gid)).select('gid_instrument','gid_respondent','gid_provider','gid_product','gid_episode','gid_wave','typ_nps','val_nps','val_nps_category','cde_nps_category','txt_verbatim')

## dim_question
df_question = spark.table(f'{silver_layer}.dim_question').filter(F.col('gid_instrument') == instrument_gid).drop('jobrunid')

## stg_fact_response
df_response = spark.sql(f"select * from {gold_layer}.st_fact_response where gid_instrument = {instrument_gid} and deleted_in_source='false' and gid_wave in ({result})")


## dim_gold_mapping
df_gold_mapping = spark.table(f'{bronze_layer}.gold_mapping').filter((F.col('nam_instrument') == 'US Hotels') & (F.col('mapping_category') == 'Episode'))

# COMMAND ----------

# MAGIC %md
# MAGIC `fact_response transformations`

# COMMAND ----------

response_filter = [
    'uid_Episode_Screener',
    'uid_Episode_Recommended'    
]

df_response_trans = df_response.join(df_question, on='gid_question', how='inner').drop('gid_question')

df_response_trans = df_response_trans.filter(F.col('id_universal_question').isin(response_filter))

# df_response_trans = df_response_trans.filter(~F.col('id_survey_question').like('%990oe%'))

#Join with episode
df_response_trans = df_response_trans.join(df_episode, on='gid_episode', how='left')
# .drop('df_episode.gid_episode')

# COMMAND ----------

df_response_trans_1 = df_response_trans.filter(F.col("id_universal_question") == "uid_Episode_Screener")


# COMMAND ----------


df_response_trans_2 = df_response_trans.filter(F.col("id_universal_question") != "uid_Episode_Screener")


# COMMAND ----------


df_response_trans_2 = df_response_trans_2.withColumn(
    "likelihood_to_recommend",
    F.when(
        F.col("txt_answer").isin("1", "2", "3", "4", "Increased significantly 5"),
        "Delight"
    ).when(
        F.col("txt_answer").isin("-1", "-2", "-3", "-4", "Decreased significantly -5"),
        "Annoy"
    ).otherwise("Neutral")
)


# COMMAND ----------


df_response_trans_3 = df_response_trans_1.join(df_response_trans_2, on=['gid_respondent','gid_provider','gid_episode','gid_wave'],how= 'left').drop('df_response_trans_2.gid_respondent','df_response_trans_2.gid_provider','df_response_trans_2.gid_episode','df_response_trans_2.gid_wave','df_response_trans_2.desc_episode','df_response_trans_1.nam_episode','df_response_trans_2.nam_episode')

df_response_trans_4 = df_response_trans_3.drop('nam_episode','desc_episode').drop_duplicates()
df_response_trans_4.count()


# COMMAND ----------

# MAGIC %md
# MAGIC `fact_nps transformations`

# COMMAND ----------

df_nps = df_nps.withColumn('episode_assigned', F.lit(1)).drop('desc_episode')

df =  df_response_trans_4.join(df_nps, on=['gid_respondent','gid_provider','gid_episode','gid_wave'], how='left').drop('df_nps_trans.gid_wave','df_response_trans_4.nam_episode')

# COMMAND ----------

# df.select('nam_episode').display()

# COMMAND ----------


## Join with dim_episode
df = df.join(
    df_episode,
    on='gid_episode',
    how='left'
).drop('df_episode.gid_episode')
    


# COMMAND ----------

## join with dim_gold_mapping
df2 = df.join(
    df_gold_mapping,
    on=F.trim(df['nam_episode']) == df_gold_mapping['map_field_value_1'],
    how='left'
).drop('df_gold_mapping.gid_product','df.nam_episode','desc_episode','nam_abbr_episode','cde_episode')


# COMMAND ----------

# rename of columns
df_final = df2.withColumnRenamed('map_field_value_2','nam_abbr_episode').withColumnRenamed('map_field_value_3','grp_episode').withColumnRenamed('map_field_value_4','desc_episode').withColumnRenamed('map_field_value_5','cde_episode')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ` joning fact_nps_trans with fact_response_trans`

# COMMAND ----------

df_final = df_final.withColumn('episode_assigned', F.when(F.col('episode_assigned').isNull(), 0).otherwise(F.col('episode_assigned')))

# COMMAND ----------

# renaming of column
df_final = df_final.withColumnRenamed('nam_abbr_episode','nam_long_episode')

# COMMAND ----------

# MAGIC %md
# MAGIC `final selection`

# COMMAND ----------

df_selected = df_final.select(
    'gid_respondent',
    'gid_provider',
    'gid_episode',
    'gid_wave',
    'typ_nps',
    'val_nps',
    'val_nps_category',
    'cde_nps_category',
    'nam_long_episode',
    'grp_episode',
    'episode_assigned',
    'likelihood_to_recommend'
)

# COMMAND ----------

df_final = df_selected.withColumn("dte_create", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC `delete old enteries and ingest new records`

# COMMAND ----------

spark.sql(f"""DELETE FROM {gold_layer}.{output_table_name} WHERE gid_wave = '{gid_wave}'""")

# COMMAND ----------

df_final.write.mode('append').saveAsTable(f"""{gold_layer}.{output_table_name}""")

# COMMAND ----------

target_table=f"{gold_layer}.{output_table_name}"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)