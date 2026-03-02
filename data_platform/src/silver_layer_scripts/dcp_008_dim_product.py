# Databricks notebook source
# DBTITLE 1,Parameters
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# DBTITLE 1,importing config file
# Imports
from pyspark.sql import functions as F
from pyspark.sql import Window

#from data_plat_cdc_logic import *
from data_plat_cdc_logic_updated import *

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# DBTITLE 1,defining silver layer table
# Creating Variables
silver_layer = "prism_silver"
bronze_layer = "prism_bronze"

canonical_tables_catalog = "canonical_tables"
decipher_catalog = "decipher"

questions_mapping_table = "questions_mapping"
answers_mapping_table = "answers_mapping"
survey_metadata_table = "survey_metadata"
dim_instrument_table = "dim_instrument"
dim_wave_table = "dim_wave"
dim_product_table = "dim_product"



# COMMAND ----------

# DBTITLE 1,fetching latest data joining metadata, dim_wave and dim_instrument
# Read survey metadata table from bronze layer
df_product = spark.read.table(f"{bronze_layer}.{decipher_catalog}.{survey_metadata_table}")

# Read answers mapping table from bronze layer, filtering for product name answers
df_answers_mapping = spark.sql(
    f"""
    SELECT survey_id,
           layout_id,
           txt_answer,
           recoded_txt_answer
    FROM {bronze_layer}.{decipher_catalog}.{answers_mapping_table}
    WHERE id_survey_question = 'nam_product' and txt_answer is not null
    """
)

# Read instrument and wave dimension tables from silver layer
df_instrument = spark.sql(f'SELECT * FROM {silver_layer}.{canonical_tables_catalog}.{dim_instrument_table}')
df_wave = spark.sql(f'SELECT * FROM {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} WHERE active = "true"')

# Join instrument and wave tables on 'gid_instrument' to get valid instrument-wave pairs
df_instrument_wave = df_instrument.join(df_wave, 'gid_instrument', how='inner').select('gid_instrument', 'survey_id', 'layout_id')

# Join product metadata with instrument-wave pairs on 'survey_id' and 'layout_id'
df_product_wave = df_product.join(df_instrument_wave, ['survey_id', 'layout_id'], how='inner')

# Join with answers mapping to get product names
df_answer_long = df_product_wave.join(df_answers_mapping, ['survey_id', 'layout_id'], how='inner')

# Select unique product names and their associated instrument IDs
df_product_final = (
    df_answer_long
    .withColumnRenamed('txt_answer', 'nam_product')\
        .withColumnRenamed('recoded_txt_answer', 'nam_product_disp')\
    .select('gid_instrument', 'nam_product','nam_product_disp')
    .dropDuplicates(['gid_instrument', 'nam_product'])
)


# COMMAND ----------

# DBTITLE 1,CDC Implementation
# If table exists, run CDC logic
if spark.catalog.tableExists(f"{silver_layer}.{canonical_tables_catalog}.{dim_product_table}"):

    dict_df = {dim_product_table: df_product_final}
    key_cols = {dim_product_table: ['nam_product','gid_instrument']}

    df_new_records = dataplatform_cdc(tables_dict = dict_df, key_cols = key_cols)

    if df_new_records.isEmpty():
        dbutils.notebook.exit("No new records found")
    else:
        df_new_records=df_new_records.withColumn("jobrunid", F.lit(job_runid))
        df_new_records.select(spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_product_table}").columns)  \  
            .write.format("delta")  \  
            .mode("append") \
            .option("mergeSchema", "true")   \  
            .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_product_table}")

# Else save new table
else:
    df_product_final = (
        df_product_final
        .withColumn('dte_create', F.current_timestamp())
        .withColumn('dte_update', F.current_timestamp())
        .withColumn('jobrunid', F.lit(job_runid))
        )

    # Adding the unique ID column
df_product_final = df_product_final.withColumn("gid_product", F.row_number().over(Window.orderBy("nam_product")))

    df_product_final \
        .write.format("delta")  \
        .mode("append")  \
        .option("mergeSchema", "true")   \  
        .saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.{dim_product_table}")
