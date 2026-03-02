# Databricks notebook source
# Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# Creating variables
# Catalogs
bronze_catalog = 'prism_bronze'
silver_catalog = 'prism_silver'

# Schemas
survey_provider_schema = 'decipher'
raw_answers_schema = 'decipher_raw_answers_long_format'
canonical_tables_schema = 'canonical_tables'

# Tables
answer_mapping_table = 'answers_mapping'
survey_metadata_table = 'survey_metadata'
long_table_name = 'stg_survey_responses_long_format'
dim_wave_table = 'dim_wave'

# COMMAND ----------

# Getting raw data
list_of_available_surveys_numbers = [item[4] for item in spark.table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}").where(F.col('active')==True).collect()]

list_of_available_surveys = [
    f"survey_responses_{item[-1]}" for item in (
    spark
    .table(f"{silver_catalog}.{canonical_tables_schema}.{dim_wave_table}").where(F.col('active')==True)
    .select('survey_id', 'layout_id')
    .withColumn('file_name', F.concat(F.col('survey_id'), F.lit("_"), F.col('layout_id'))))
    .collect()
    ]

# Added filter for existing data
df_survey_metadata = [responses_file_name[0] for responses_file_name in spark.sql("select distinct concat('survey_responses_', survey_id, '_', layout_id) filename from prism_bronze.decipher.survey_metadata").collect()]

list_of_available_surveys = list(set(list_of_available_surveys) & set(df_survey_metadata))

# COMMAND ----------

if len(list_of_available_surveys) == 0:

    dbutils.notebook.exit("No data to process.")

# COMMAND ----------

# Creating a loop to ingest and treat all raw_responses tables
for table_name in list_of_available_surveys:

    # Creating a list of cols to drop
    list_of_cols_to_drop = [
        'row_hash', 
        'bronze_updated_at', 
        'file_name', 
        '__START_AT', 
        '__END_AT', 
        '_rescued_data'
        ]
    
    list_of_pivot_indices = ["uuid", "survey_id", "layout_id"]

    # Ingesting answers_mapping_file
    df_answers_mapping = (
        spark
        .table(f"{bronze_catalog}.{survey_provider_schema}.{answer_mapping_table}")
        .where(F.col('__END_AT').isNull())
        .select("id_survey_question", 'num_answer', 'survey_id', 'layout_id', 'txt_answer')
        )

    # Checking for duplicates
    assert (
        df_answers_mapping
        .groupBy('id_survey_question', 'num_answer', 'survey_id', 'layout_id')
        .count()
        .where(F.col('count')>1)
        .count()
        ) == 0

    # Ingesting raw_responses table and selecting relevant columns
    table_df = (
        spark
        .table(f"{bronze_catalog}.{raw_answers_schema}.{table_name}")
        .drop(*list_of_cols_to_drop)
        .withColumnRenamed('value', 'num_answer')
        )

    # Joining both DataFrames
    # Joining both DataFrames
    df_table_final = (
        table_df
        .join(
            df_answers_mapping.select(
                F.col('id_survey_question').alias('mapping_id_survey_question'),
                F.col('num_answer').alias('mapping_num_answer'),
                F.col('survey_id').alias('mapping_survey_id'),
                F.col('layout_id').alias('mapping_layout_id'),
                'txt_answer'
            ),
            on=(
                (table_df.id_survey_question.eqNullSafe(F.col('mapping_id_survey_question'))) &
                (table_df.num_answer.eqNullSafe(F.col('mapping_num_answer'))) &
                (table_df.survey_id.eqNullSafe(F.col('mapping_survey_id'))) &
                (table_df.layout_id.eqNullSafe(F.col('mapping_layout_id')))
            ),
            how='left'
        )
        .withColumn('txt_answer', F.when(F.col('txt_answer').isNull(), F.col('num_answer')).otherwise(F.col('txt_answer')))
        .withColumn('dte_create', F.current_timestamp())  
        .withColumn('dte_update', F.current_timestamp())
        .withColumn('deleted_in_source', F.lit(False))
        .withColumn('type', F.lit(None).cast('string'))
        .drop('mapping_id_survey_question', 'mapping_num_answer', 'mapping_survey_id', 'mapping_layout_id')
        )
    
    assert table_df.count() == df_table_final.count()

    # Checking to see if table already exists
    # If not, save the table to the silver catalog with long_table_name as name
    if not spark.catalog.tableExists(f"{silver_catalog}.{canonical_tables_schema}.{long_table_name}"):

        # Saving initial version of the table
        (
        df_table_final
        .write
        .option("mergeSchema", "true")
        .saveAsTable(f"{silver_catalog}.{canonical_tables_schema}.{long_table_name}")
        )

    # If the table exists, perform merge respecting SCD Type 1
    else:

        pass

        # Reading existing table as Delta Table
        target_table = DeltaTable.forName(spark, f"{silver_catalog}.{canonical_tables_schema}.{long_table_name}")

        # Selecting columns to merge
        merge_columns = ['uuid', 'id_survey_question', 'survey_id', 'layout_id']

        # Control columns
        control_columns = ['dte_create', 'dte_update', 'deleted_in_source']

        # Selecting columns to be updated
        update_columns = [column for column in df_table_final.columns if column not in merge_columns + control_columns] # all other columns

        # Creating merge conditions - these are the columns that must match for upsert to happen
        merge_conditions = " AND ".join([f"target.{col} = source.{col}" for col in merge_columns])

        # Creating conditions for when there are updated values to be upserted
        not_matched_condition =  " OR ".join([f"target.{col} != source.{col}" for col in update_columns])

        # Getting the current source survey_id and layout_id for handling deletions
        source_survey_id = df_table_final.select('survey_id').distinct().collect()[0][0]
        source_layout_id = df_table_final.select('layout_id').distinct().collect()[0][0]

        # Deletion conditions - To update deleted_in_source just records of the survey that is being merged
        deletion_condition = f"target.survey_id = '{source_survey_id}' AND target.layout_id = '{source_layout_id}'"

        # Creating dictionaries
        # Creating insert dict - For when there are new valuesd
        insert_dict = {
            col: f"source.{col}" 
            for col in merge_columns + update_columns
            } | {
            "dte_create": "current_timestamp()",
            "dte_update": "current_timestamp()",
            "deleted_in_source": "false"
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
                df_table_final.alias("source"),
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
                condition=deletion_condition,
                set=deletion_dict
            )
        )

        # Executing merge
        merge_operation.execute()

        
