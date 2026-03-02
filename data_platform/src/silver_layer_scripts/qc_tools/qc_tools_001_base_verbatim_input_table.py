# Databricks notebook source
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# Creating variables
# Catalogs
silver_catalog = 'prism_silver'

# Schemas
canonical_tables_schema = 'canonical_tables'
qc_tools_tables_schema = 'qc_tools_tables'

# Tables
fact_response_table = 'fact_response'
dim_question_table = 'dim_question'
qct_input_verbatim_table = 'qct_input_base_verbatim'


# COMMAND ----------

# Read input tables
dim_question = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_question_table}")
fact_response = spark.table(f"{silver_catalog}.{canonical_tables_schema}.{fact_response_table}")

# COMMAND ----------

# Prepare dim_question
# Filter questions that are marked as verbatim and that have open ended questions

dim_question_clean = dim_question \
    .where(F.col("typ_question") == "text") \
    .where(F.lower(F.col("id_universal_question")).contains("verbatim")) \
    .select('gid_question')

# dim_question_clean.display()

# COMMAND ----------

# Filter fact_response to get only verbatim responses
fact_response_clean = fact_response.where(F.col("deleted_in_source") == False).join(dim_question_clean, on="gid_question")

# fact_response_clean.display()

# COMMAND ----------

relevant_cols = [c for c in fact_response_clean.columns if c.startswith('gid_')] +['txt_answer']
qct_input_verbatim = fact_response_clean \
    .select(relevant_cols) \
    .withColumn('dte_create', F.current_timestamp()) \
    .withColumn('dte_update', F.current_timestamp())

# qct_input_verbatim.display()

# COMMAND ----------

# Writing table

if not spark.catalog.tableExists(f"{silver_catalog}.{qc_tools_tables_schema}.{qct_input_verbatim_table}"):

    # df_fact_response_final.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'{silver_catalog}.{canonical_tables_schema}.{qct_input_verbatim_table}')
    qct_input_verbatim.write.saveAsTable(f'{silver_catalog}.{qc_tools_tables_schema}.{qct_input_verbatim_table}')

else:

    # Reading existing table as Delta Table
    target_table = DeltaTable.forName(spark, f"{silver_catalog}.{qc_tools_tables_schema}.{qct_input_verbatim_table}")

    # Selecting columns to merge
    merge_columns = [column for column in qct_input_verbatim.columns if 'gid_' in column]

    # Control columns
    control_columns = ['dte_create', 'dte_update']

    # Selecting columns to be updated
    update_columns = [column for column in qct_input_verbatim.columns if column not in merge_columns + control_columns] 

    # UPDATED: Use merge_key for merge conditions
    # merge_conditions = "target.merge_key = source.merge_key"
    merge_conditions = " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])

    # Creating conditions for when there are updated values to be upserted
    # not_matched_condition = f"target.row_checksum != source.row_checksum" 
    not_matched_condition = " OR ".join([f"target.{col} != source.{col}" for col in update_columns])

    # Creating dictionaries - include salt columns
    # Creating insert dict - For when there are new values
    insert_dict = {
        col: f"source.{col}" 
        for col in merge_columns + update_columns
        } | {
        "dte_create": "current_timestamp()",
        "dte_update": "current_timestamp()",
        "deleted_in_source": "false",
        "gid_response": "uuid()"
        }

    # Creating update dict
    update_dict = {
        col: f"source.{col}" 
        for col in update_columns
        } | {
        "dte_update": "current_timestamp()",
        }

    # Creating deletion dict
    deletion_dict = {
        f"target.deleted_in_source" : "true",
        "dte_update": "current_timestamp()" 
        }

    # Merging tables
    merge_operation = (
        target_table.alias("target")
        .merge(
            qct_input_verbatim.alias("source"),
            merge_conditions
            )
        )
        
    # Merging when there are updated and when there are new values
    merge_operation = (
        merge_operation
        .whenNotMatchedInsert(
            values=insert_dict
        )
        .whenMatchedUpdate(
            condition=not_matched_condition,
            set=update_dict
        )
        .whenNotMatchedBySourceUpdate(
            set=deletion_dict
        )
    )
    
    # Executing merge
    merge_operation.execute()