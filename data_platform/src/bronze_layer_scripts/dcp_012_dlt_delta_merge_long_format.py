# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Get list of surveys to process
surveys_df = spark.table("prism_bronze.data_plat_control.surveys")
surveys_and_layouts_ids_list = [(row.survey_id, row.layout_id) for row in surveys_df.collect()]

# COMMAND ----------

def table_exists(table_name):
    """
    Check if a table exists
    """
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False

def merge_melted_to_main(survey_id, layout_id):
    """
    Merge data from melted table into main survey responses table
    """
    source_table = f'prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}_melted'
    target_table = f'prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}'
    
    # Check if source table exists
    if not table_exists(source_table):
        print(f"Source table does not exist: {source_table}")
        return
    
    # Check if target table exists, if not create it
    if not table_exists(target_table):
        print(f"Target table does not exist, creating: {target_table}")
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
        
        schema = StructType([
            StructField("survey_id", StringType(), True),
            StructField("layout_id", StringType(), True),
            StructField("uuid", StringType(), True),
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("dte_create", TimestampType(), True),
            StructField("dte_update", TimestampType(), True),
            StructField("deleted_in_source", BooleanType(), True)
        ])
        
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"Created target table: {target_table}")
    
    # Check if source table has data
    source_count = spark.table(source_table).count()
    if source_count == 0:
        print(f"No data to merge in {source_table}")
        return
    
    print(f"Starting merge for {survey_id}_{layout_id}")
    print(f"  Source records: {source_count}")
    
    # Perform the merge
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING {source_table} AS source
    ON target.uuid = source.uuid 
       AND target.survey_id = source.survey_id 
       AND target.layout_id = source.layout_id
       AND target.key = source.key
    WHEN MATCHED THEN UPDATE SET 
        target.value = source.value,
        target.dte_update = current_timestamp(),
        target.deleted_in_source = false
    WHEN NOT MATCHED THEN INSERT (
        survey_id,
        layout_id,
        uuid,
        key,
        value,
        dte_create,
        dte_update,
        deleted_in_source
    ) VALUES (
        source.survey_id,
        source.layout_id,
        source.uuid,
        source.key,
        source.value,
        source.dte_create,
        source.dte_update,
        false
    )
    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
        target.deleted_in_source = true,
        target.dte_update = current_timestamp()
    """
    
    spark.sql(merge_sql)
    
    target_count = spark.table(target_table).count()
    print(f"  Merge completed. Target table now has {target_count} records")

    print("Truncating melted table - required because truncation cannot happen within DLT")
    spark.sql(f"TRUNCATE TABLE {source_table}")
    
    # Optional: Truncate source table after successful merge
    # spark.sql(f"TRUNCATE TABLE {source_table}")
    # print(f"  Source table truncated")

# Process all surveys
for survey_id, layout_id in surveys_and_layouts_ids_list:
    try:
        merge_melted_to_main(survey_id, layout_id)
    except Exception as e:
        print(f"Error processing {survey_id}_{layout_id}: {str(e)}")
        continue

print("All merges completed")