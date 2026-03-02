# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
jobrunid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

from data_plat_cdc_logic import dataplatform_cdc
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`
# MAGIC

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Variables
silver_catalog = 'prism_silver'
silver_schema = 'canonical_tables'
target_table = 'fact_nps'

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

## wave table
df_wave = spark.table('prism_silver.canonical_tables.dim_wave').filter(F.col('active')==True).select('gid_instrument','gid_wave')


## Filtering records from the fact_response where gid_wave = 'active' and deleted_in_source = 'deleted_in_source'
raw_data = spark.table('prism_silver.canonical_tables.fact_response').filter(F.col('deleted_in_source')=='false')
raw_data = raw_data.join(df_wave, ['gid_instrument','gid_wave'], 'inner')

## dim_question table
df_question = spark.table('prism_silver.canonical_tables.dim_question').select("gid_question",'id_survey_question','id_universal_question')

# COMMAND ----------

# MAGIC %md
# MAGIC `Filteration of Respondent Questions`

# COMMAND ----------

df_question_nps = df_question.filter(
    F.col("id_universal_question").ilike('%NPS')
)

df_question_verbatim = df_question.filter(
    F.col("id_universal_question").ilike('%NPS_Verbatim')
)

# COMMAND ----------

df_nps = raw_data.join(
    df_question_nps,
    on='gid_question',
    how='inner'
)

df_nps = df_nps.withColumn(
    'typ_nps',
    F.when(
        F.col('id_universal_question') == 'uid_Provider_NPS',
        'Provider NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Product_NPS',
        'Product NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Episode_NPS',
        'Episode NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Channel_NPS',
        'Channel NPS'
    ).when(
        F.col('id_survey_question') == 'Q_Loyalty_Program_NPS_1',
        'Loyalty NPS 1'
    ).when(
        F.col('id_survey_question') == 'Q_Loyalty_Program_NPS_2',
        'Loyalty NPS 2'
    ).otherwise(None)
)

df_nps = df_nps.withColumnRenamed('num_answer', 'val_nps').withColumnRenamed('gid_question', 'gid_question_nps')

df_verbatim = raw_data.join(
    df_question_verbatim,
    on='gid_question',
    how='inner'
).withColumn(
    'typ_nps',
    F.when(
        F.col('id_universal_question') == 'uid_Provider_NPS_Verbatim',
        'Provider NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Product_NPS_Verbatim',
        'Product NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Episode_NPS_Verbatim',
        'Episode NPS'
    ).when(
        F.col('id_universal_question') == 'uid_Channel_NPS_Verbatim',
        'Channel NPS'
    ).when(
        F.col('id_survey_question') == 'Q_Loyalty_Program_NPS_Verbatim_1',
        'Loyalty NPS 1'
    ).when(
        F.col('id_survey_question') == 'Q_Loyalty_Program_NPS_Verbatim_2',
        'Loyalty NPS 2'
    ).otherwise(None)
)

df_verbatim = df_verbatim.withColumnRenamed('txt_answer', 'txt_verbatim').withColumnRenamed('gid_question', 'gid_question_verbatim')

# COMMAND ----------

join_cols = [col for col in df_nps.columns if 'gid' in col and col not in ['gid_response', 'gid_question_nps', 'gid_question_nps', 'gid_answer_recoded']] + ['typ_nps']
# join_cols = ['typ_nps','gid_respondent','gid_provider','gid_product','gid_episode','gid_channel','gid_instrument','gid_wave']
df_input = df_nps.join(
    df_verbatim,
    on=join_cols,
    how='left'
).drop(df_verbatim['gid_instrument'],'dte_create','dte_update','jobrunid')

# COMMAND ----------

list_of_nps_vals = [str(item) for item in range(11)]

df_new_records_derived = df_input.withColumn('val_nps', F.col('val_nps').cast('int'))  \
                    .withColumn("val_nps_category", F.when(F.col("val_nps").isin(list_of_nps_vals[:7]), F.lit(-1))
                                                    .when(F.col("val_nps").isin(list_of_nps_vals[7:9]), F.lit(0))
                                                    .otherwise(1))  \
                    .withColumn("cde_nps_category", F.when(F.col("val_nps").isin(list_of_nps_vals[:7]), F.lit("Detractor"))
                                                    .when(F.col("val_nps").isin(list_of_nps_vals[7:9]), F.lit("Passive"))
                                                    .otherwise("Promoter")) \
                    .withColumn('dte_create',F.current_timestamp()).repartition(200)

max_gid = int(spark.sql(f'Select coalesce(max(gid_npsfact), 0) as max_gid from {silver_catalog}.{silver_schema}.{target_table}').collect()[0][0])

window_spec = Window.orderBy("gid_respondent")
df_new_records_derived = df_new_records_derived.withColumn("gid_npsfact", max_gid + F.row_number().over(window_spec))

# COMMAND ----------

# DBTITLE 1,Gid_nps-fact  mapping
from pyspark.sql import functions as F, Window

#  add a deterministic tie-breaker so ordering doesn't fluctuate when values repeat
df_new_records_derived = df_new_records_derived.withColumn("_tie", F.monotonically_increasing_id())

w = Window.orderBy(
    F.col("gid_wave"),
    F.col("txt_verbatim"),
    F.col("gid_respondent"),
    F.col("_tie")
)

df_new_records_derived = (
    df_new_records_derived
    .withColumn("gid_npsfact", F.lit(max_gid) + F.row_number().over(w))
    .withColumn("jobrunid", F.lit(jobrunid))
    .withColumn("dte_update", F.current_timestamp())
    .drop("_tie")
)


# COMMAND ----------

# DBTITLE 1,Final Selection
df_new_records_select = df_new_records_derived.select( 
 'gid_npsfact',
 'gid_instrument',
 'gid_wave',
 'gid_respondent',
 'gid_provider',
 'gid_product',
 'gid_episode',
 'gid_channel',
 'gid_initial_channel',
 'gid_final_channel',
 'gid_question_nps',
 'gid_question_verbatim',
 'typ_nps',
 'val_nps',
 'val_nps_category',
 'cde_nps_category',
 'txt_verbatim',
 'dte_create',
 'dte_update',
 'jobrunid'
)

# COMMAND ----------

# DBTITLE 1,Merge and Insert
# Delete and append updated data if table already exists
# Get active wave IDs for the specified instrument
waves_to_delete = spark.sql(
    f"""
    SELECT w.gid_wave
    FROM {silver_catalog}.{silver_schema}.dim_wave w
    INNER JOIN {silver_catalog}.{silver_schema}.dim_instrument i 
    ON w.gid_instrument = i.gid_instrument
    WHERE w.active = 'true'
    """
)

if waves_to_delete.count() == 0:
    print(f"No active waves found")
    dbutils.notebook.exit("No active waves found. Exiting notebook.")

else:
    # Convert wave IDs to a comma-separated string for SQL IN clause
    str_waves_to_waves = ', '.join(map(str, list(map(lambda a:a.gid_wave, waves_to_delete.collect()))))

    # Delete records from the target table for the selected waves
    spark.sql(
        f"""
        DELETE FROM {silver_catalog}.{silver_schema}.{target_table}
        WHERE gid_wave IN ({str_waves_to_waves})
        """ )

    df_new_records_select.write.mode('append').saveAsTable(f'{silver_catalog}.{silver_schema}.{target_table}')