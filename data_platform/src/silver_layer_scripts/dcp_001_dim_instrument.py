# Databricks notebook source
from data_plat_cdc_logic import dataplatform_cdc
from pyspark.sql import functions as F

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

raw_data = spark.table('prism_bronze.decipher.all_surveys') 
raw_data = raw_data.select('title','tags').filter(raw_data.__END_AT.isNull())
raw_data = raw_data.drop_duplicates()
raw_data = raw_data.filter(F.expr("exists(tags, x -> x like 'nam_instrument:%')"))
surveys_df = spark.table('prism_bronze.data_plat_control.surveys').select('survey_name')


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
    .withColumn("tag_value", F.split("tag", ":")[1])

raw_data = raw_data.drop('tags','tag')


# COMMAND ----------

# MAGIC %md
# MAGIC `Transformations : Pivot  - Columns creation from tagging keys`

# COMMAND ----------

# DBTITLE 1,Pivot  - Columns creation from tagging keys
pivoted_data = raw_data.groupBy("title").pivot("tag_key").agg(F.expr("first(tag_value)"))

# COMMAND ----------

# MAGIC %md
# MAGIC `Transformations : Trimming and Initial selection`

# COMMAND ----------

# DBTITLE 1,Trimming and Initial selection
columns = [
    'title',
    'nam_instrument',
    'nam_region',
    'nam_country',
    'nam_industry',
    'sub_industry',
    'instrument_type',
    'instrument_scope',
    'programming_platform',
    'instrument_description'
]

pivoted_data = pivoted_data.select(
    *[F.trim(col).alias(col) for col in columns]
)


# COMMAND ----------

# MAGIC %md
# MAGIC `Final Selection`

# COMMAND ----------

# DBTITLE 1,Final Selection
final_df = pivoted_data.select(
    "nam_instrument",
    "nam_region",
    "nam_country",
    "nam_industry",
    "sub_industry",
    "instrument_type",
    "instrument_scope",
    "programming_platform",
    "instrument_description"
)

final_df = final_df.drop_duplicates()


# COMMAND ----------

# MAGIC %md
# MAGIC `Key columns for CDC`

# COMMAND ----------

# DBTITLE 1,Key columns for CDC
dict_df = {"dim_instrument": final_df}
key_cols = {'dim_instrument': ['nam_instrument','nam_country']}

# COMMAND ----------

# MAGIC %md
# MAGIC `CDC Implementation`

# COMMAND ----------

# DBTITLE 1,CDC Implementation
df_new_records = dataplatform_cdc(tables_dict =dict_df,key_cols = key_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC `Creation of extra columns`

# COMMAND ----------

# DBTITLE 1,cde_major_taxonomy  creation
df_new_records = df_new_records.withColumn("cde_major_taxonomy", F.lit(None))


# COMMAND ----------

# MAGIC %md
# MAGIC `Inserting new records with final selection from the table`

# COMMAND ----------

# DBTITLE 1,Inserting new records
if df_new_records.isEmpty():
    dbutils.notebook.exit("No new records found")
else:
     table_columns = [field.name for field in spark.table(f"{silver_layer}.dim_instrument").schema.fields]
     df_to_insert = df_new_records.select(*table_columns)
     df_to_insert.write.mode("append").saveAsTable(f"{silver_layer}.dim_instrument")

# COMMAND ----------

# MAGIC %md
# MAGIC `Table Creation with CDF Enabled`

# COMMAND ----------

# DBTITLE 1,Table Creation with CDF Enabled
# %sql
# -- Create dim_instrument Table in prism_silver.default with Change Data Feed Enabled
# CREATE TABLE IF NOT EXISTS prism_silver.canonical_tables.dim_instrument ( 
#   gid_instrument INTEGER,
#   cde_major_taxonomy STRING,
#   nam_instrument STRING,
#   nam_region STRING,
#   nam_country STRING,
#   nam_industry STRING,
#   sub_industry STRING,
#   instrument_type STRING,
#   instrument_scope STRING,
#   programming_platform STRING,
#   instrument_description STRING,
#   dte_update TIMESTAMP,
#   dte_create TIMESTAMP )
#   TBLPROPERTIES (
#     delta.enableChangeDataFeed = true
# );