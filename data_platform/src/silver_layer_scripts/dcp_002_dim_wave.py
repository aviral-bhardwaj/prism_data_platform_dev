# Databricks notebook source
from data_plat_cdc_logic import dataplatform_cdc
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,func:derive_wave_fields
from wave_utils import derive_wave_fields


# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`
# MAGIC

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Variables
silver_layer = 'prism_silver.canonical_tables'

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

# DBTITLE 1,Sources
df_instrument = spark.table('prism_silver.canonical_tables.dim_instrument').select('gid_instrument','nam_instrument','nam_country') 
raw_data = spark.table('prism_bronze.decipher.all_surveys')
raw_data = raw_data.select('title', 'tags', 'dateLaunched', 'closedDate','state').filter(raw_data.__END_AT.isNull())
raw_data = raw_data.drop_duplicates()
raw_data = raw_data.filter(F.expr("exists(tags, x -> x like 'nam_instrument:%')"))
surveys_df = spark.table('prism_bronze.data_plat_control.surveys').select('survey_name', 'layout_id', 'survey_id').withColumnRenamed('layout_id', 'layout_id_control_surveys').withColumnRenamed('survey_id', 'survey_id_control_surveys')

# COMMAND ----------

# MAGIC %md
# MAGIC `Filtering only correct records`

# COMMAND ----------

raw_data = raw_data.join(surveys_df, on = (raw_data.title == surveys_df.survey_name), how = 'inner')

# COMMAND ----------

# MAGIC %md
# MAGIC `Transformations : Tagging Split into Key & Value`

# COMMAND ----------

# DBTITLE 1,Tagging Split into Key  & Value
raw_data = raw_data.withColumn("tag", F.explode("tags"))\
    .withColumn("tag_key", F.split("tag", ":")[0])\
    .withColumn("tag_value", F.split("tag", ":")[1])\
    .withColumn("dateLaunched", F.to_date("dateLaunched"))\
    .withColumn("closedDate", F.to_date("closedDate"))\
    .withColumn("active", F.lit(True))\
    .drop('layout_id')

raw_data = raw_data.drop('tags','tag')


# COMMAND ----------

# MAGIC %md
# MAGIC `Transformations : Pivot  - Columns creation from tagging keys`

# COMMAND ----------

# DBTITLE 1,Pivot  - Columns creation from tagging keys
pivoted_data = raw_data.groupBy("title",'dateLaunched','closedDate','state', 'layout_id_control_surveys', 'survey_id_control_surveys', 'active').pivot("tag_key").agg(F.expr("first(tag_value)"))


# COMMAND ----------

# MAGIC %md
# MAGIC `Getting layout_id from config file and not tags`

# COMMAND ----------

pivoted_data = (
    pivoted_data
    .withColumn('layout_id', F.col('layout_id_control_surveys'))
    .withColumn('survey_id', F.col('survey_id_control_surveys'))
    .drop('layout_id_control_surveys', 'survey_id_control_surveys')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC `Transformations : Trimming and Initial selection and data type change`

# COMMAND ----------

# DBTITLE 1,Trimming and Initial selection
pivoted_data = pivoted_data.select(
    F.trim('title').alias('title'),
    F.trim('nam_instrument').alias('nam_instrument'),
    F.trim('nam_country').alias('nam_country'),
    F.trim(F.col('survey_id').cast('string')).alias('survey_id'),
    F.trim(F.col('layout_id').cast('string')).alias('layout_id'),
    F.trim('wave').alias('desc_wave'),
    F.trim('instrument_detail').alias('survey_phase'),
    F.to_date(F.col('dateLaunched')).alias('wave_dte_start'),
    F.to_date(F.col('closedDate')).alias('wave_dte_end'),
    F.trim(F.col('state').cast('string')).alias('survey_state'),
    F.col('active').alias('active')
)


# COMMAND ----------

# MAGIC %md
# MAGIC `Joining with dim_instrument to get gid_instrument`

# COMMAND ----------

join_df = pivoted_data.join(
    df_instrument, 
    on=['nam_instrument', 'nam_country'], 
    how='inner'
).drop('nam_instrument', 'nam_country')


# COMMAND ----------

# MAGIC %md
# MAGIC `Final Selection`

# COMMAND ----------

# DBTITLE 1,Final Selection
final_df = join_df.select(
  'gid_instrument',
  'survey_id',
 'layout_id',
 'desc_wave',
 'survey_phase',
 'wave_dte_start',
 'wave_dte_end',
 'survey_state',
 'active'
)
final_df=derive_wave_fields(final_df,'desc_wave')
final_df = final_df.drop_duplicates()


# COMMAND ----------

# MAGIC %md
# MAGIC `Key columns for CDC`

# COMMAND ----------

# DBTITLE 1,Key columns for CDC
dict_df = {"dim_wave": final_df}
key_cols = {'dim_wave': ['gid_instrument','survey_id','survey_phase']}

# COMMAND ----------

# MAGIC %md
# MAGIC `CDC Implementation`

# COMMAND ----------

# DBTITLE 1,CDC Implementation
df_new_records = dataplatform_cdc(tables_dict =dict_df,key_cols = key_cols,exclusion_cols=['active'])

# COMMAND ----------

# MAGIC %md
# MAGIC `Inserting new records`

# COMMAND ----------

# DBTITLE 1,Inserting new records
if df_new_records.isEmpty():
    dbutils.notebook.exit("No new records found")
else:
     table_columns = [field.name for field in spark.table(f"{silver_layer}.dim_wave").schema.fields]
     df_to_insert = df_new_records.select(*table_columns)
     df_to_insert.write.mode("append").saveAsTable(f"{silver_layer}.dim_wave")
     

# COMMAND ----------

# MAGIC %md
# MAGIC `Table Creation with CDF Enabled`

# COMMAND ----------

# DBTITLE 1,Table Creation with CDF Enabled
# %sql
# -- Create dim_instrument Table in prism_silver.default with Change Data Feed Enabled
# CREATE TABLE IF NOT EXISTS prism_silver.canonical_tables.dim_wave ( 
#   gid_wave INTEGER,
#   gid_instrument INTEGER,
#   desc_wave STRING,
#   survey_phase STRING,
#   survey_id STRING,
#   layout_id STRING,
#   wave_dte_start DATE,
#   wave_dte_end DATE,
#   survey_state STRING,
#   dte_update TIMESTAMP,
#   dte_create TIMESTAMP )
#   TBLPROPERTIES (
#     delta.enableChangeDataFeed = true
# );