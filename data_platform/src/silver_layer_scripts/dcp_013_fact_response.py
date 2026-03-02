# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
jobrunid = dbutils.widgets.get("jobrunid")
print(jobrunid)

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from pyspark import StorageLevel

from delta.tables import DeltaTable

# COMMAND ----------

# Creating variables
# Catalogs
bronze_catalog = 'prism_bronze'
silver_catalog = 'prism_silver'

# Schemas
survey_provider_schema = 'decipher'
raw_answers_schema = 'decipher_raw_answers'
canonical_tables_schema = 'canonical_tables'

# Tables
long_table_name = 'stg_survey_responses_long_format'
dim_variable_mapping_table = 'dim_variable_mapping'
fact_response_table = 'fact_response'
dim_wave_table = 'dim_wave'
dim_instrument_table = 'dim_instrument'
dim_question_table = 'dim_question'
dim_channel_table = 'dim_channel'
dim_episode_table = 'dim_episode'
dim_product_table = 'dim_product'

# TBD
dim_provider_table = 'dim_provider'
# dim_product_table = 'dim_product'
dim_respondent_table = 'dim_respondent'
dim_answer_recoded_table = 'dim_answer_recoded'


# COMMAND ----------

# Creating DataFrames for Dim Tables
df_dim_wave = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}").where(F.col('active')==True).select('gid_wave', 'gid_instrument', 'survey_id', 'layout_id')
df_dim_question = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}").select('gid_question', 'gid_instrument', 'id_survey_question')
df_dim_channel = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_channel_table}").select('gid_channel', 'gid_instrument', 'nam_channel')
df_dim_episode = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_episode_table}").select('gid_episode', 'gid_instrument', 'nam_episode')

# To be incorporated when tables are ready
df_dim_provider = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_provider_table}").select('gid_provider', 'gid_instrument', 'nam_provider')
df_dim_product = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_product_table}").select('gid_product', 'gid_instrument', 'nam_product')
df_dim_respondent = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_respondent_table}").select('id_respondent', 'gid_respondent')
df_dim_answer_recoded = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_answer_recoded_table}").select('gid_answer_recoded', 'gid_instrument','id_survey_question', 'txt_answer', 'num_answer')

df_survey_details = spark.sql(f"""
                                SELECT DISTINCT
                                    dvm.gid_instrument,
                                    dvm.gid_variable_mapping,
                                    dvm.id_question AS id_survey_question,
                                    provider AS id_survey_response_provider,
                                    product AS id_survey_response_product,
                                    episode AS id_survey_response_episode,
                                    channel AS id_survey_response_channel,
                                    initial_channel AS id_survey_response_initial_channel,
                                    final_channel AS id_survey_response_final_channel
                                FROM 
                                    {silver_catalog}.{canonical_tables_schema}.{dim_variable_mapping_table} dvm
                              """)

df_long_table=spark.sql(f"""
                       select ssr.* ,dw.gid_wave,dw.gid_instrument from {silver_catalog}.{canonical_tables_schema}.{long_table_name} ssr
inner join {silver_catalog}.{canonical_tables_schema}.{dim_wave_table} dw 
  on ssr.survey_id = dw.survey_id 
  and ssr.layout_id = dw.layout_id
  where dw.active=True and ssr.deleted_in_source=False
  """)


# COMMAND ----------

# Checking to see if there is data to me processed
if df_dim_wave.count() == 0:

    dbutils.notebook.exit("No data to process.")

# COMMAND ----------

# Hard coding to account for encoding, this should be eliminated in the future
df_dim_answer_recoded = df_dim_answer_recoded.withColumn('txt_answer', F.when(F.col('txt_answer').contains(r'â€™'), F.regexp_replace('txt_answer', r'â€™', '’')).otherwise(F.col('txt_answer')))
df_dim_answer_recoded = df_dim_answer_recoded.withColumn('txt_answer', F.when(F.col('txt_answer').contains(r'Ã©'), F.regexp_replace('txt_answer', r'Ã©', 'é')).otherwise(F.col('txt_answer')))

# COMMAND ----------

# Creating the equivalent of stg_clean_raw

# First join
df_result = (
    df_long_table
    .join(
        df_survey_details,
        on=['gid_instrument', 'id_survey_question'],
        how='left'
    )
)
#print(df_result.count())

# Define the columns that need to be joined back to get text answers (excluding episode)
response_columns = [
    'id_survey_response_provider',
    'id_survey_response_product', 
    'id_survey_response_episode',
    'id_survey_response_channel',
    'id_survey_response_initial_channel',
    'id_survey_response_final_channel'
]

# Create aliases for the text answer columns
text_column_names = [col.replace('id_survey_response_', '') for col in response_columns]

# Perform joins for each response column
for i, response_col in enumerate(response_columns):
    text_col_name = text_column_names[i]
    
    # Create a DataFrame for this specific response type
    df_response = (
        df_long_table
        .select('uuid', 'survey_id', 'layout_id', 'id_survey_question', 'txt_answer', 'num_answer')
        .withColumnRenamed('txt_answer', text_col_name)
        .alias(f'{text_col_name}_df')
    )
    
    # Join with the main result DataFrame
    df_result = (
        df_result.alias('main')
        .join(
            df_response,
            on=[
                F.col('main.uuid') == F.col(f'{text_col_name}_df.uuid'),
                F.col('main.survey_id') == F.col(f'{text_col_name}_df.survey_id'),
                F.col('main.layout_id') == F.col(f'{text_col_name}_df.layout_id'),
                F.col(f'main.{response_col}') == F.col(f'{text_col_name}_df.id_survey_question')
            ],
            how='left'
        )
        .select(
            'main.*',  # Select all columns from main
            F.col(f'{text_col_name}_df.{text_col_name}')  # Select the renamed text answer
        )
    )

# Final selection: keep only desired columns and rename id_survey_response_episode to episode
df_final = (
    df_result
    .withColumn('provider', F.coalesce(F.col('provider'), F.col('id_survey_response_provider')))
    .withColumn('product', F.coalesce(F.col('product'), F.col('id_survey_response_product')))
    .withColumn('episode', F.coalesce(F.col('episode'), F.col('id_survey_response_episode')))
    .withColumn('channel', F.coalesce(F.col('channel'), F.col('id_survey_response_channel')))
    .withColumn('initial_channel', F.coalesce(F.col('initial_channel'), F.col('id_survey_response_initial_channel')))
    .withColumn('final_channel', F.coalesce(F.col('final_channel'), F.col('id_survey_response_final_channel')))
    .select(
    'uuid',
    'survey_id',
    'layout_id', 
    'id_survey_question',
    'provider',
    'product',
    'episode', 
    'channel',
    'initial_channel',
    'final_channel',
    'txt_answer',
    'num_answer'
    ))


# COMMAND ----------

# Mapping GIDs from dim tables
df_table_final = (
    df_final
    .join(
        df_dim_wave,
        on=['survey_id', 'layout_id'],
        how='inner'
        )
    .withColumn('gid_wave', F.coalesce(F.col('gid_wave'), F.lit(-1)))
    .withColumn('gid_instrument', F.coalesce(F.col('gid_instrument'), F.lit(-1)))
    .join(
        df_dim_answer_recoded,
        on=['id_survey_question', 'gid_instrument', 'id_survey_question', 'txt_answer', 'num_answer'],
        how='left'
        )
    .withColumn('gid_answer_recoded', F.coalesce(F.col('gid_answer_recoded'), F.lit(-1)))
    .join(
        df_dim_question,
        on=['id_survey_question', 'gid_instrument'],
        how='left'
        )
    .withColumn('gid_question', F.coalesce(F.col('gid_question'), F.lit(-1)))
    .join(
        df_dim_channel.withColumnRenamed('nam_channel', 'channel'),
        on=['gid_instrument', 'channel'],
        how='left'
        )
    .withColumn('gid_channel', F.coalesce(F.col('gid_channel'), F.lit(-1)))
    .join(
        df_dim_channel.withColumnsRenamed({'nam_channel': 'initial_channel', 'gid_channel': 'gid_initial_channel'}),
        on=['gid_instrument', 'initial_channel'],
        how='left'
        )
    .withColumn('gid_initial_channel', F.coalesce(F.col('gid_initial_channel'), F.lit(-1)))
    .join(
        df_dim_channel.withColumnsRenamed({'nam_channel': 'final_channel', 'gid_channel': 'gid_final_channel'}),
        on=['gid_instrument', 'final_channel'],
        how='left'
        )
    .withColumn('gid_final_channel', F.coalesce(F.col('gid_final_channel'), F.lit(-1)))
    .join(
        df_dim_provider.withColumnRenamed('nam_provider', 'provider'),
        on=['provider', 'gid_instrument'],
        how='left'
        )
    .withColumn('gid_provider', F.coalesce(F.col('gid_provider'), F.lit(-1)))
    .join(
        df_dim_episode.withColumnRenamed('nam_episode', 'episode'),
        on=['gid_instrument', 'episode'],
        how='left'
        )
    .withColumn('gid_episode', F.coalesce(F.col('gid_episode'), F.lit(-1)))
    .join(
        df_dim_product.withColumnRenamed('nam_product', 'product'),
        on=['gid_instrument', 'product'],
        how='left'
        )
    .withColumn('gid_product', F.coalesce(F.col('gid_product'), F.lit(-1)))
    .join(
        df_dim_respondent.withColumnRenamed('id_respondent', 'uuid'),
        on=['uuid'],
        how='left'
        )
    .withColumn('gid_sub_product', F.lit(-1))
    .withColumn('gid_sub_question', F.lit(-1))
    .withColumn('dte_create', F.current_timestamp())
    .withColumn('dte_update', F.current_timestamp())
    .withColumn('deleted_in_source', F.lit(False))
    .drop('layout_id', 'survey_id', 'uuid', 'id_survey_question', 'channel', 'initial_channel', 'final_channel', 'episode', 'provider', 'product')
    )

# Creating hash column and checksum for performing the merge
composite_key_hash_columns = [col for col in df_table_final.columns if 'gid_' in col and col != 'gid_response']
row_hash_columns = [col for col in df_table_final.columns if col not in ['dte_create', 'dte_update', 'deleted_in_source']]


# COMMAND ----------

# Dynamic Select - For aesthetic purposes
main_columns = ['gid_respondent', 'gid_provider', 'gid_product', 'gid_episode', 'gid_channel', 'gid_initial_channel', 'gid_final_channel', 'gid_answer_recoded']
control_columns = ['dte_create', 'dte_update', 'deleted_in_source']
gid_columns = sorted([column for column in df_table_final.columns if 'gid_' in column and column not in main_columns + control_columns])
other_columns = sorted([column for column in df_table_final.columns if column and column not in main_columns + gid_columns + control_columns])

final_columns_list = main_columns + gid_columns + other_columns + control_columns
final_columns_list

df_fact_response_final = df_table_final.select(final_columns_list)


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

jobrunid_value = jobrunid  # <- set this from your job context

target_table_name = f"{silver_catalog}.{canonical_tables_schema}.{fact_response_table}"

# if table doesn't exist, create it (with gid_response + jobrunid + control cols)
if not spark.catalog.tableExists(target_table_name):

    df_fact_response_final = (
        df_fact_response_final
        .withColumn("gid_response", F.expr("uuid()"))
        .withColumn("jobrunid", F.lit(jobrunid_value))
        .withColumn("dte_create", F.current_timestamp())
        .withColumn("dte_update", F.current_timestamp())
        .withColumn("deleted_in_source", F.lit(False))
    )

    df_fact_response_final.write.format("delta").saveAsTable(target_table_name)

else:
    target_table = DeltaTable.forName(spark, target_table_name)

    # merge keys (gid_* columns)
    merge_columns = [c for c in df_fact_response_final.columns if c.startswith("gid_")]

    # control columns you do NOT want in "data compare"
    control_columns = ["dte_create", "dte_update", "deleted_in_source", "gid_response", "jobrunid"]

    # columns that represent business/data fields to compare and update
    update_columns = [c for c in df_fact_response_final.columns if c not in merge_columns + control_columns]

    # merge condition on all gid_* keys
    merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in merge_columns])

    # updated condition: any data column changed OR record was previously deleted
    # (null-safe compare is better than != because of nulls)
    changed_exprs = [f"NOT (target.{c} <=> source.{c})" for c in update_columns]
    not_matched_condition = " OR ".join(changed_exprs + ["target.deleted_in_source = true"])

    # scope deletions only to waves in this run
    gid_wave_ids = [row[0] for row in df_fact_response_final.select("gid_wave").distinct().collect()]
    gid_wave_ids_sql = ", ".join([f"'{x}'" for x in gid_wave_ids])  # quote strings

    deletion_condition = f"target.gid_wave IN ({gid_wave_ids_sql})"

    # -------------------------
    # 4) Insert/Update/Delete dicts (jobrunid updates on insert + update)
    # -------------------------
    insert_dict = {c: f"source.{c}" for c in merge_columns + update_columns} | {
        "dte_create": "current_timestamp()",
        "dte_update": "current_timestamp()",
        "deleted_in_source": "false",
        "gid_response": "uuid()",
        "jobrunid": f"'{jobrunid}'"
    }

    update_dict = {c: f"source.{c}" for c in update_columns} | {
        "dte_update": "current_timestamp()",
        "deleted_in_source": "false",          # if it was deleted before and comes back, revive it
        "jobrunid": f"'{jobrunid_value}'"      # update jobrunid when row changes
    }

    deletion_dict = {
        "deleted_in_source": "true",
        "dte_update": "current_timestamp()",
        "jobrunid": f"'{jobrunid}'"      # optional: track jobrunid for delete marking too
    }
    
    
    # execute merge
    (
        target_table.alias("target")
        .merge(df_fact_response_final.alias("source"), merge_condition)
        .whenNotMatchedInsert(values=insert_dict)
        .whenMatchedUpdate(condition=not_matched_condition, set=update_dict)
        .whenNotMatchedBySourceUpdate(condition=deletion_condition, set=deletion_dict)
        .execute()
    )