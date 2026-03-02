# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
#from data_plat_cdc_logic import *
from data_plat_cdc_logic_updated import *

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
dim_ansewr_recoded_table='dim_answer_recoded'

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

# DBTITLE 1,Sources
df_wave = spark.table('prism_silver.canonical_tables.dim_wave').select('gid_instrument','survey_id').where(F.col('active')==True)
raw_data = spark.table('prism_bronze.decipher.answers_mapping').filter(F.col('__END_AT').isNull())


# COMMAND ----------

# MAGIC %md
# MAGIC `Joining with dim_instrument to get gid_instrument`

# COMMAND ----------

join_df = raw_data.join(
    df_wave, 
    on=['survey_id'], 
    how='inner'
).drop(df_wave.survey_id)


# COMMAND ----------

# Recoding recoded_num_answer to force it being integer

join_df = join_df.withColumn("recoded_num_answer", F.regexp_replace("recoded_num_answer", r"\.0$", ""))

# COMMAND ----------

# MAGIC %md
# MAGIC `Final Selection`

# COMMAND ----------

# DBTITLE 1,Final Selection
final_df = join_df.select(
 'gid_instrument',
 'id_survey_question',
 'txt_answer',
 'num_answer',
 'recoded_txt_answer',
 'recoded_num_answer',
)

final_df = final_df.drop_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC `CDC Implementation`

# COMMAND ----------

# DBTITLE 1,CDC Implementation
# If table exists, run CDC logic
if spark.catalog.tableExists(f"{silver_layer}.{dim_ansewr_recoded_table}"):
    #final_df = final_df.withColumn("jobrunid", F.lit(job_runid))
    dict_df = {dim_ansewr_recoded_table: final_df}
    key_cols = {dim_ansewr_recoded_table: ['gid_instrument','id_survey_question', 'txt_answer', 'num_answer']}
    


    df_new_records = dataplatform_cdc(tables_dict = dict_df, key_cols = key_cols)


    if df_new_records.isEmpty():
        dbutils.notebook.exit("No new records found")
    else:
        df_new_records = df_new_records.withColumn("jobrunid", F.lit(job_runid))
        df_new_records.select(spark.table(f"{silver_layer}.{dim_ansewr_recoded_table}").columns)  \
        .write.format("delta")  \
        .mode("append") \
        .option("mergeSchema", "true")   \
        .saveAsTable(f"{silver_layer}.{dim_ansewr_recoded_table}")

# Else save new table
else:
    final_df = (
        final_df
        .withColumn('dte_create', F.current_timestamp())
        .withColumn('dte_update', F.current_timestamp())
        .withColumn("jobrunid", F.lit(job_runid))
        )

    # Adding the unique ID column
    final_df = final_df.withColumn("gid_answer_recoded", F.row_number().over(Window.orderBy("recoded_txt_answer")))

    final_df \
        .write.format("delta")  \
        .mode("append") \
        .option("mergeSchema", "true")   \
        .saveAsTable(f"{silver_layer}.{dim_ansewr_recoded_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC `Table Creation with CDF Enabled`

# COMMAND ----------

# DBTITLE 1,Table Creation with CDF Enabled
# %sql
# -- Create dim_answer_recoded Table in prism_silver.default with Change Data Feed Enabled
# CREATE TABLE IF NOT EXISTS prism_silver.canonical_tables.dim_answer_recoded ( 
#   gid_answer_recoded  INTEGER,
#   gid_instrument INTEGER,
#   id_survey_question STRING,
#   txt_answer STRING,
#   num_answer STRING,
#   recoded_txt_answer STRING,
#   recoded_num_answer STRING,
#   dte_update TIMESTAMP,
#   dte_create TIMESTAMP )
#   TBLPROPERTIES (
#     delta.enableChangeDataFeed = true
# );