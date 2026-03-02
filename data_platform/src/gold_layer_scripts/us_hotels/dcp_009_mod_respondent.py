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
# MAGIC `Importing Dependencies`

# COMMAND ----------

from pyspark.sql import functions as F
from linkage_table import *

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`

# COMMAND ----------

gold_schema = 'prism_gold.us_hotels'
silver_schema = 'prism_silver.canonical_tables'
target_table = 'mod_respondent'
instrument = 'US Hotels'

# COMMAND ----------


spark.sql(f"DELETE FROM prism_gold.us_hotels.mod_respondent where gid_wave in ({result})")

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

# DBTITLE 1,Input tables
df_instrument = spark.table(f"{silver_schema}.dim_instrument").filter(
    F.col("nam_instrument") == instrument)
    
gid_intrument = df_instrument.select('gid_instrument').collect()[0]['gid_instrument']

df_wave = spark.table(f"{silver_schema}.dim_wave") \
    .filter(F.col("gid_instrument") == gid_intrument) \
    .filter(F.expr(f"gid_wave in ({result})"))
latest_wave_dte = df_wave.agg(F.max('wave_dte_start').alias('wave_dte_start')).collect()[0]['wave_dte_start']
gid_wave = df_wave.filter(F.col("wave_dte_start") == latest_wave_dte).select('gid_wave').collect()[0]['gid_wave']
desc_wave = df_wave.filter(F.col("gid_wave") == gid_wave).select('desc_wave').collect()[0]['desc_wave']

df_respondent = spark.table(f"{silver_schema}.dim_respondent") \
    .filter(F.col("gid_instrument") == gid_intrument) \
    .filter(F.expr(f"gid_wave in ({result})")) \
    .filter((F.col("qualified_status") != "Partial") & (F.col("quality_start") == "Yes"))

                         

df_question = spark.table(f"""{silver_schema}.dim_question""")\
    .filter(F.col("gid_instrument") == gid_intrument)\
            .filter(F.trim(F.lower(F.col("id_survey_question"))) == "target_phase")

gid_target_phase_quest = df_question.select("gid_question").collect()[0]['gid_question']

df_response = spark.table(f"""{gold_schema}.st_fact_response""")\
                   .filter(F.col("gid_instrument") == gid_intrument)\
                   .filter(F.expr(f"gid_wave in ({result})"))\
                   .filter(F.col("gid_question") == gid_target_phase_quest).select("gid_respondent","txt_answer")\
                   .filter(F.col('deleted_in_source')=='false')

# COMMAND ----------

# DBTITLE 1,Transformation and logic
df_response = df_response.groupBy("gid_respondent").agg(F.first("txt_answer").alias("txt_answer"))
df_combined = df_respondent.alias("df_respondent").join(df_response.alias("df_response"), ["gid_respondent"],"left")
df_combined = df_combined.select("df_respondent.*", "df_response.txt_answer")
df_combined = df_combined.withColumn("sampling_phase", F.when(F.col("sampling_phase") == "Rep", F.col("sampling_phase"))
                                                .when(F.col("txt_answer") == "Provider X Purpose", "OS - Purpose").otherwise("OS - Provider"))

# COMMAND ----------

df_combined = df_combined.withColumn("desc_wave", F.lit(desc_wave))\
                         .withColumn("dte_create", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC `final selection`

# COMMAND ----------

df_combined = df_combined.select("gid_instrument",
                                 "gid_wave",
                                 "gid_respondent",
                                 "id_respondent",
                                 "desc_wave",
                                 "age_range",
                                 "gender",
                                 "qualified_status",
                                 "sampling_phase",
                                 "termination_code",
                                 "income_range",
                                 "region",
                                 "state",
                                 "zip_code",
                                 "dte_create")

# COMMAND ----------

# MAGIC %md
# MAGIC `delete old enteries and ingest new records`

# COMMAND ----------

# DBTITLE 1,Data push
df_combined.write.mode('append').saveAsTable(f"""{gold_schema}.{target_table}""")

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC create table prism_gold.us_hotels.mod_respondent(
# MAGIC                                  gid_instrument integer,
# MAGIC                                  gid_wave integer,
# MAGIC                                  gid_respondent integer,
# MAGIC                                  id_respondent string,
# MAGIC                                  desc_wave string,
# MAGIC                                  age_range string,
# MAGIC                                  gender string,
# MAGIC                                  qualified_status string,
# MAGIC                                  sampling_phase string,
# MAGIC                                  termination_code string,
# MAGIC                                  income_range string,
# MAGIC                                  region string,
# MAGIC                                  state string,
# MAGIC                                  zip_code string,
# MAGIC                                  dte_create timestamp)

# COMMAND ----------

target_table="prism_gold.us_hotels.mod_respondent"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)