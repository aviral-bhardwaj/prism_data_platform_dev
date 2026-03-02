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

# Imports
from pyspark.sql import functions as F
from pyspark import StorageLevel
from linkage_table import *
from delta.tables import DeltaTable

# COMMAND ----------

# Creating variables
# Catalogs
bronze_catalog = 'prism_bronze'
silver_catalog = 'prism_silver'
gold_catalog = 'prism_gold'

# Schemas
survey_provider_schema = 'decipher'
raw_answers_schema = 'decipher_raw_answers'
canonical_tables_schema = 'canonical_tables'
hotels_schema = 'us_hotels'

# Tables
fact_nps_silver_table = 'fact_nps'
fact_nps_gold_table = 'st_fact_nps'
dim_respondent_silver_table = 'dim_respondent'


# COMMAND ----------

spark.sql(f"DELETE from {gold_catalog}.{hotels_schema}.{fact_nps_gold_table} where gid_wave in ({result})")

# COMMAND ----------

# Creating pertinent DataFrames
df_fact_nps =spark.sql(f" SELECT * from {silver_catalog}.{canonical_tables_schema}.{fact_nps_silver_table} where where gid_wave in ({result})") 

df_dim_respondent =spark.sql(f" select gid_respondent, qualified_status, quality_start from {silver_catalog}.{canonical_tables_schema}.{dim_respondent_silver_table} where gid_wave in ({result})")


# COMMAND ----------

# Joining and filtering both dataframes
df_final = (
    df_fact_nps
    .join(df_dim_respondent, on='gid_respondent', how='left')
    .filter(
        (F.col('gid_instrument')==2) 
        & (
            (F.col('qualified_status')!='Partial') 
            & (F.col('quality_start')=="Yes")
            )
        )
        .drop('qualified_status', 'quality_start')
    )

# COMMAND ----------

# # Writing table

# if not spark.catalog.tableExists(f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}"):

#     # df_fact_nps_final.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'{silver_catalog}.{canonical_tables_schema}.{fact_nps_table}')
#     df_final.write.saveAsTable(f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}")

# else:

#     # Reading existing table as Delta Table
#     target_table = DeltaTable.forName(spark, f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}")

#     # First, add salt columns to target table if they don't exist
#     target_df = spark.table(f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}")
    
#     # Selecting columns to merge
#     merge_columns = [column for column in df_final.columns if 'gid_' in column]

#     # Control columns
#     control_columns = ['dte_create']

#     # Selecting columns to be updated
#     update_columns = [column for column in df_final.columns if column not in merge_columns + control_columns] 

#     # UPDATED: Use merge_key for merge conditions
#     # merge_conditions = "target.merge_key = source.merge_key"
#     merge_conditions = " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])

#     # Creating conditions for when there are updated values to be upserted
#     # not_matched_condition = f"target.row_checksum != source.row_checksum" 
#     not_matched_condition = " OR ".join([f"target.{col} != source.{col}" for col in update_columns])

#     # Creating dictionaries - include salt columns
#     # Creating insert dict - For when there are new values
#     insert_dict = {
#         col: f"source.{col}" 
#         for col in merge_columns + update_columns
#         } | {
#         "dte_create": "current_timestamp()",
#         }

#     # Creating update dict
#     update_dict = {
#         col: f"source.{col}" 
#         for col in update_columns
#         } | {
#         }

#     # Merging tables
#     merge_operation = (
#         target_table.alias("target")
#         .merge(
#             df_final.alias("source"),
#             merge_conditions
#             )
#         )
        
#     # Merging when there are updated and when there are new values
#     merge_operation = (
#         merge_operation
#         .whenNotMatchedInsert(
#             values=insert_dict
#         )
#         .whenMatchedUpdate(
#             condition=not_matched_condition,
#             set=update_dict
#         )
#         .whenNotMatchedBySourceDelete(
#         )
#     )
    
#     # Executing merge
#     merge_operation.execute()


# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable

target_table_name = f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}"

target_table = DeltaTable.forName(spark, target_table_name)

target_cols = set(spark.table(target_table_name).columns)
source_cols = set(df_final.columns)

# only gid_ cols present on both sides
merge_columns = [c for c in df_final.columns if c.startswith("gid_") and c in target_cols]

control_columns = ["dte_create"]

update_columns = [c for c in df_final.columns
                  if c not in merge_columns + control_columns and c in target_cols]

merge_conditions = " AND ".join([f"target.{c} = source.{c}" for c in merge_columns])

not_matched_condition = " OR ".join([f"target.{c} != source.{c}" for c in update_columns]) if update_columns else None

insert_cols = [c for c in (merge_columns + update_columns) if c in target_cols]

insert_dict = {c: f"source.{c}" for c in insert_cols}
insert_dict["dte_create"] = "current_timestamp()"

update_dict = {c: f"source.{c}" for c in update_columns}

merge_op = (
    target_table.alias("target")
    .merge(df_final.alias("source"), merge_conditions)
    .whenNotMatchedInsert(values=insert_dict)
)

if update_dict:
    merge_op = merge_op.whenMatchedUpdate(
        condition=not_matched_condition,
        set=update_dict
    )

merge_op.execute()


# COMMAND ----------

target_table=f"{gold_catalog}.{hotels_schema}.{fact_nps_gold_table}"

insert_data(
  spark=spark,
  table_name="prism_bronze.data_plat_control.job_run_details",
  target_table_name=target_table,
  notebook_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1],
  jobrunid=jr,
  jobid=jid,
  username=un
)