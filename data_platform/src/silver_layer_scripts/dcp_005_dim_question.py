# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# DBTITLE 1,importing config file
# Imports
from pyspark.sql import functions as F
#from data_plat_cdc_logic import *
from data_plat_cdc_logic_updated import *

# COMMAND ----------

# DBTITLE 1,varriable
silver_layer = 'prism_silver.canonical_tables'
silver_layer = 'prism_silver.canonical_tables'
# Creating Variables
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_catalog = "canonical_tables"
decipher_catalog = "decipher"

dim_question_table = "dim_question"



# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,raw data
raw_data = spark.table('prism_bronze.decipher.survey_metadata').filter((F.col('__END_AT').isNull()))
raw_data = raw_data.select('label','qlabel','qtitle','rowTitle','type','survey_id')

wave_df = spark.table(f"prism_silver.canonical_tables.dim_wave").where(F.col('active')==True)

raw_data = raw_data.alias('raw_data').join(wave_df.alias('wave_df'),['survey_id'],'inner')
raw_data = raw_data.select('raw_data.*','wave_df.gid_instrument')


gid_instrument_list = [row['gid_instrument'] for row in raw_data.select('gid_instrument').distinct().collect()]

question_mapping = spark.table('prism_bronze.decipher.questions_mapping')
question_mapping = question_mapping.filter(F.col('__END_AT').isNull())

# COMMAND ----------

# DBTITLE 1,Column renaming
raw_data = raw_data.withColumnRenamed('label','id_survey_question')\
                   .withColumnRenamed('qlabel','nam_question')\
                   .withColumnRenamed('qtitle','disp_question')\
                   .withColumnRenamed('rowTitle','question_header_row')\
                   .withColumnRenamed('type','typ_question')\
                   .withColumn('sub_typ_question',F.lit(None))

# COMMAND ----------

# DBTITLE 1,join with question_mapping
raw_data = raw_data.alias('raw_data').join(question_mapping.alias('question_mapping'),['id_survey_question', 'survey_id'],'left')

raw_data = raw_data.select('raw_data.*','question_mapping.question_header_column','question_mapping.id_universal_question','question_mapping.desc_question_grp','question_mapping.disp_question_recoded','question_mapping.question_header_row_recoded','question_mapping.question_header_column_recoded').drop('survey_id').distinct().drop('nam_instrument','nam_country')

# COMMAND ----------

# DBTITLE 1,CDC Implementation
from pyspark.sql.window import Window
# If table exists, run CDC logic
if spark.catalog.tableExists(f"{silver_layer}.{canonical_tables_catalog}.{dim_question_table}"):

    dict_df = {"dim_question": raw_data}
    key_cols = {'dim_question': ['id_survey_question', 'gid_instrument']}

    df_new_records = dataplatform_cdc(tables_dict = dict_df, key_cols = key_cols)
    col_names = df_new_records.columns
    col_names = [i for i in col_names if i.startswith('gid')]
    print(col_names)
    for i in col_names:
        df_new_records = df_new_records.withColumn(i, F.when(F.col(i).isNull(), "-1").otherwise(F.col(i)))
        df_new_records = df_new_records.withColumn(i, df_new_records[i].cast("integer"))

    if df_new_records.isEmpty():
        dbutils.notebook.exit("No new records found")
    else:
        df_new_records=df_new_records.withColumn("jobrunid", F.lit(job_runid))
        df_new_records.select(spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_question_table}").columns)  \
            .write.format("delta")  \
            .mode("append") \
            .option("mergeSchema", "true")   \
            .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_question_table}")

# Else save new table
else:
    raw_data = (
        raw_data
        .withColumn('dte_create', F.current_timestamp())
        .withColumn('dte_update', F.current_timestamp())
        .withColumn('jobrunid', F.lit(job_runid))
        )

    # Adding the unique ID column
    raw_data = raw_data.withColumn("gid_question", F.row_number().over(Window.orderBy("nam_question")))

    raw_data \
        .write.format("delta")  \
        .mode("append") \
        .option("mergeSchema", "true")   \
        .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_question_table}")