# Databricks notebook source
# MAGIC %md
# MAGIC # Survey Response Processing with SCD Type 2 and File Tracking - UPDATED
# MAGIC
# MAGIC This notebook processes survey response JSON files with:
# MAGIC - **SCD Type 2** for tracking historical changes
# MAGIC - **File Tracking** to avoid reprocessing already processed files
# MAGIC - **Hybrid bulk merge approach** (parallel batching + single merge)
# MAGIC - **Memory-efficient streaming** using ijson
# MAGIC - **PARALLEL TABLE PROCESSING** for multiple surveys concurrently
# MAGIC
# MAGIC ## Key Improvements:
# MAGIC 1. Delta table-based file processing log
# MAGIC 2. Automatic skip of already-processed files
# MAGIC 3. Retry capability for failed files
# MAGIC 4. Full audit trail of processing history
# MAGIC 5. **Parallel batch processing to temp table + single bulk merge**
# MAGIC 6. **Fast parallel batching + eliminates ConcurrentAppendException**
# MAGIC 7. **NEW: Parallel survey/layout processing**
# MAGIC 8. **NEW: Complete snapshot CDC with proper deletion handling**
# MAGIC 9. **NEW: Renamed 'key' column to 'id_survey_question'**

# COMMAND ----------

# MAGIC %pip install ijson polars

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from pathlib import Path
import polars as pl
import ijson
import json
import os
import sys
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from delta.tables import DeltaTable
import time


# COMMAND ----------

# Adding utils directory to the Python path
def add_utils_to_path(utils_dir_name="utils", max_up=10):
    cur = os.path.normpath(os.getcwd())

    for _ in range(max_up):
        candidate = os.path.join(cur, utils_dir_name)
        if os.path.isdir(candidate):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            print(f"Added {candidate} to Python path")
            return candidate
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent

    raise RuntimeError(f"Could not find '{utils_dir_name}' within {max_up} parent levels from cwd: {os.getcwd()}")

add_utils_to_path()

# COMMAND ----------

# importing utils
from logging_utils import log_file_processing_start, log_file_processing_complete
import logging_utils

# COMMAND ----------

# Configuration
json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_responses_endpoint/'

# Load config from file
with open('config.json', 'r') as f:
    config = json.load(f)

# File tracking configuration
FILE_LOG_TABLE = config['file_log_table']
REPROCESS_FAILED = config['reprocess_failed']
FILE_LOG_TABLE = config['file_log_table']

# Parallel processing configuration
MAX_PARALLEL_SURVEYS = 3  # Number of survey/layout combinations to process in parallel

# Set the global variables in the logging_utils module
logging_utils.spark = spark
logging_utils.dbutils = dbutils
logging_utils.FILE_LOG_TABLE = FILE_LOG_TABLE

# COMMAND ----------

surveys_df = spark.table("prism_bronze.data_plat_control.surveys")
surveys_and_layouts_ids_list = [(row.survey_id, row.layout_id) for row in surveys_df.collect()]

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: File Discovery and Tracking Functions

# COMMAND ----------

def get_json_files(base_path, survey_id, layout_id):
    """
    Get all JSON files for a survey/layout combination.
    Returns list of file paths with metadata.
    """
    dir_path = Path(base_path) / f"{survey_id}_{layout_id}"
    
    if not dir_path.exists():
        return []
    
    json_files = []
    for file_path in dir_path.glob("*.json"):
        stat = file_path.stat()
        json_files.append({
            'path': str(file_path),
            'name': file_path.name,
            'size_bytes': stat.st_size,
            'modification_time': datetime.fromtimestamp(stat.st_mtime)
        })
    
    return json_files

# COMMAND ----------

def get_processed_files(survey_id=None, layout_id=None):
    """
    Get set of already processed file paths from the log table.
    Only returns files with status='SUCCESS' unless REPROCESS_FAILED is True.
    """
    query = f"SELECT file_path, status FROM {FILE_LOG_TABLE} WHERE 1=1"
    
    if survey_id:
        query += f" AND survey_id = '{survey_id}'"
    if layout_id:
        query += f" AND layout_id = '{layout_id}'"
    
    try:
        processed_df = spark.sql(query)
        
        if REPROCESS_FAILED:
            # Only skip successfully processed files
            processed_files = set(
                row.file_path for row in processed_df.filter(F.col("status") == "SUCCESS").collect()
            )
        else:
            # Skip all previously processed files (success or failed)
            processed_files = set(row.file_path for row in processed_df.collect())
        
        return processed_files
    except Exception as e:
        print(f"Warning: Could not read processed files: {e}")
        return set()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: File Processing Function with Updated CDC Logic

# COMMAND ----------

def process_large_file_streaming(file_path, survey_id, layout_id, batch_size=2000, max_workers=3):
    """
    Process JSON file using BULK merge approach with parallel batch processing.

    Args:
        file_path: Path to the JSON file
        survey_id: Survey identifier
        layout_id: Layout identifier
        batch_size: Number of respondents per batch (default: 2000)
        max_workers: Maximum parallel write threads (default: 3)
    
    Returns:
        tuple: (respondent_count, total_records_inserted)
    """

    from pyspark.sql import types as T
    
    melted_table_name = f'prism_bronze.decipher_raw_answers_long_format.survey_responses_{survey_id}_{layout_id}'
    temp_table_name = f'prism_bronze.decipher_raw_answers_long_format.survey_responses_{survey_id}_{layout_id}_temp_{int(time.time())}'
    current_timestamp = datetime.now()
    
    # Define schema for Delta table with SCD Type 2 columns
    # UPDATED: 'key' renamed to 'id_survey_question'
    schema = T.StructType([
        T.StructField("survey_id", T.StringType(), True),
        T.StructField("layout_id", T.StringType(), True),
        T.StructField("uuid", T.StringType(), True),
        T.StructField("id_survey_question", T.StringType(), True),
        T.StructField("value", T.StringType(), True),
        T.StructField("dte_create", T.TimestampType(), True),
        T.StructField("dte_update", T.TimestampType(), True),
        T.StructField("__START_AT", T.TimestampType(), True),
        T.StructField("__END_AT", T.TimestampType(), True)
    ])
    
    # Check if final table exists
    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {melted_table_name}")
        table_exists = True
        print(f"[{survey_id}_{layout_id}] Using existing Delta table: {melted_table_name}")
    except Exception:
        print(f"[{survey_id}_{layout_id}] Final table does not exist yet: {melted_table_name}")
    
    # Create temporary table
    print(f"[{survey_id}_{layout_id}] Creating temporary table: {temp_table_name}")
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").saveAsTable(temp_table_name)
    
    def write_batch_to_temp(batch_data):
        """
        Write a batch to TEMPORARY table (simple append, no lock).
        Delta Lake handles concurrent appends natively - no lock needed!
        """
        batch_num, df_batch, total_respondents = batch_data
        
        try:
            # Convert Polars DataFrame to Spark DataFrame
            df_spark = spark.createDataFrame(df_batch.to_pandas())
            
            # Add SCD Type 2 columns
            df_spark = df_spark \
                .withColumn("__START_AT", F.lit(current_timestamp)) \
                .withColumn("__END_AT", F.lit(None).cast(T.TimestampType()))
            
            # Append to temp table - NO LOCK! Delta handles concurrent appends.
            df_spark.write.format("delta").mode("append").saveAsTable(temp_table_name)
            
            return batch_num, total_respondents, None
        except Exception as e:
            return batch_num, total_respondents, str(e)
    
    counter = 0
    batch_num = 0
    melted_dfs = []
    write_tasks = []
    
    print(f"\n[{survey_id}_{layout_id}] Processing file with BULK merge approach (parallel batching): {file_path}")
    print(f"[{survey_id}_{layout_id}] Batch size: {batch_size} respondents, Max workers: {max_workers}")
    print(f"[{survey_id}_{layout_id}] Business Keys: survey_id, layout_id, uuid, id_survey_question")
    print(f"\n[{survey_id}_{layout_id}] Step 1: Processing batches in parallel and appending to temp table...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        # Stream JSON file using ijson for memory efficiency
        with open(file_path, 'rb') as f:
            try:
                for resp in ijson.items(f, 'item'):
                    uuid = resp.get('uuid', '')
                    
                    # Convert response to long format
                    # OPTIMIZED: No str() conversion - keep raw types
                    # UPDATED: 'key' renamed to 'id_survey_question'
                    df_long = pl.DataFrame({
                        'survey_id': [survey_id] * len(resp),
                        'layout_id': [layout_id] * len(resp),
                        'uuid': [uuid] * len(resp),
                        'id_survey_question': list(resp.keys()),
                        'value': list(resp.values())  # Keep raw types, Spark will handle conversion
                    })
                    
                    melted_dfs.append(df_long)
                    counter += 1
                    
                    # Write batch when size threshold reached
                    if counter % batch_size == 0:
                        df_batch = pl.concat(melted_dfs)
                        batch_num += 1
                        
                        future = executor.submit(write_batch_to_temp, (batch_num, df_batch, counter))
                        write_tasks.append(future)
                        
                        print(f"[{survey_id}_{layout_id}]   Batch {batch_num}: {counter} respondents processed, submitted to temp table")
                        melted_dfs = []  # Clear memory
                
                # Process final batch if any records remain
                if melted_dfs:
                    df_batch = pl.concat(melted_dfs)
                    batch_num += 1
                    future = executor.submit(write_batch_to_temp, (batch_num, df_batch, counter))
                    write_tasks.append(future)
                    print(f"[{survey_id}_{layout_id}]   Final batch {batch_num}: {counter} respondents submitted")
            
            except Exception as e:
                print(f"[{survey_id}_{layout_id}] Error during streaming: {e}")
                import traceback
                print(traceback.format_exc())
                # Clean up temp table before raising
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
                except:
                    pass
                raise
        
        # Wait for all writes to temp table to complete
        print(f"\n[{survey_id}_{layout_id}]   Waiting for all batch appends to complete...")
        errors = []
        for future in as_completed(write_tasks):
            completed_batch, total_resp, error = future.result()
            if error:
                errors.append(f"Batch {completed_batch}: {error}")
                print(f"[{survey_id}_{layout_id}]   ✗ Batch {completed_batch} failed: {error}")
            else:
                print(f"[{survey_id}_{layout_id}]   ✓ Batch {completed_batch} completed (up to {total_resp} respondents)")
        
        if errors:
            print(f"\n[{survey_id}_{layout_id}] ⚠ {len(errors)} batch(es) encountered errors:")
            for error in errors:
                print(f"[{survey_id}_{layout_id}]   - {error}")
            # Clean up temp table
            try:
                spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
            except:
                pass
            raise Exception(f"File processing failed with {len(errors)} batch errors")
    
    # Verify temp table contents
    temp_df = spark.table(temp_table_name)
    temp_count = temp_df.count()
    print(f"\n[{survey_id}_{layout_id}] ✓ All batches appended to temp table: {temp_count:,} records")
    
    # Step 2: Merge temp table to final table (or create final table if it doesn't exist)
    if not table_exists:
        # Final table doesn't exist - just rename temp table
        print(f"\n[{survey_id}_{layout_id}] Step 2: Final table does not exist, promoting temp table to final table...")
        
        # Read from temp and write to final
        temp_df.write.format("delta").mode("overwrite").saveAsTable(melted_table_name)
        total_inserted = temp_count
        
        print(f"[{survey_id}_{layout_id}] ✓ Created final table with {temp_count:,} records")
    else:
        # Final table exists - perform SCD2 merge
        print(f"\n[{survey_id}_{layout_id}] Step 2: Performing SCD Type 2 merge from temp to final table...")
        
        delta_table = DeltaTable.forName(spark, melted_table_name)
        
        # Get current active records from target BEFORE any updates
        print(f"[{survey_id}_{layout_id}]   Reading active records from target...")
        active_before_merge = spark.table(melted_table_name) \
            .filter(F.col("__END_AT").isNull()) \
            .select(
                F.col("survey_id"),
                F.col("layout_id"),
                F.col("uuid"),
                F.col("id_survey_question"),
                F.col("value").alias("existing_value"),
                F.col("dte_update").alias("existing_dte_update")
            )
        
        # STEP 2a: Merge operation with UPDATED CDC logic
        print(f"[{survey_id}_{layout_id}]   Step 2a: Performing merge (update changed records, close deleted records)...")

        # UPDATED: Use whenNotMatchedBySourceUpdate for complete snapshot logic
        delta_table.alias("target").merge(
            temp_df.alias("source"),
            """
            target.survey_id = source.survey_id AND
            target.layout_id = source.layout_id AND
            target.uuid = source.uuid AND
            target.id_survey_question = source.id_survey_question AND
            target.__END_AT IS NULL
            """
        ).whenMatchedUpdate(
            # Close records where value changed
            condition="target.value != source.value",
            set={"__END_AT": "source.__START_AT"}
        ).whenNotMatchedBySourceUpdate(
            # UPDATED: Close ALL active records not in source (complete snapshot logic)
            # This handles:
            # 1. Deleted questions within existing UUIDs
            # 2. Completely deleted UUIDs
            # No condition needed - let the merge join keys handle the scope
            set={"__END_AT": F.lit(current_timestamp)}
        ).execute()

        print(f"[{survey_id}_{layout_id}]   ✓ Merge operation completed")
        
        # STEP 2b: Identify records to insert (NEW or CHANGED only, no duplicates)
        print(f"[{survey_id}_{layout_id}]   Step 2b: Identifying new/changed records...")
        records_to_insert = temp_df.alias("new").join(
            active_before_merge.alias("existing"),
            on=["survey_id", "layout_id", "uuid", "id_survey_question"],
            how="left"
        ).filter(
            # NEW records: business key doesn't exist in target
            F.col("existing.existing_value").isNull() |
            # CHANGED records: business key exists but value is different
            (
                (F.col("new.value") != F.col("existing.existing_value"))
            )
        ).select(
            F.col("new.survey_id"),
            F.col("new.layout_id"),
            F.col("new.uuid"),
            F.col("new.id_survey_question"),
            F.col("new.value"),
            F.col("new.dte_create"),
            F.col("new.dte_update"),
            F.col("new.__START_AT"),
            F.col("new.__END_AT")
        )
        
        # Insert only new/changed records
        records_count = records_to_insert.count()
        print(f"[{survey_id}_{layout_id}]   ✓ Found {records_count:,} new/changed records to insert")
        
        total_inserted = 0
        if records_count > 0:
            print(f"[{survey_id}_{layout_id}]   Step 2c: Inserting records...")
            records_to_insert.write.format("delta").mode("append").saveAsTable(melted_table_name)
            total_inserted = records_count
            print(f"[{survey_id}_{layout_id}]   ✓ Inserted {records_count:,} records")
        else:
            print(f"[{survey_id}_{layout_id}]   ✓ No new/changed records to insert (all duplicates)")
    
    # Step 3: Drop temporary table
    print(f"\n[{survey_id}_{layout_id}] Step 3: Cleaning up temporary table...")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
        print(f"[{survey_id}_{layout_id}] ✓ Dropped temporary table: {temp_table_name}")
    except Exception as e:
        print(f"[{survey_id}_{layout_id}] ⚠ Warning: Could not drop temp table: {e}")
    
    # Show SCD Type 2 statistics
    print(f"\n[{survey_id}_{layout_id}] Step 4: Gathering final statistics...")
    total_records = spark.table(melted_table_name).count()
    active_records = spark.table(melted_table_name).filter(F.col("__END_AT").isNull()).count()
    historical_records = total_records - active_records
    
    print(f"\n[{survey_id}_{layout_id}] {'='*60}")
    print(f"[{survey_id}_{layout_id}] Processing completed: {counter:,} respondents processed")
    print(f"[{survey_id}_{layout_id}] Table: {melted_table_name}")
    print(f"[{survey_id}_{layout_id}] Total records inserted this run: {total_inserted:,}")
    print(f"[{survey_id}_{layout_id}] Total records in table (including history): {total_records:,}")
    print(f"[{survey_id}_{layout_id}] Active records (__END_AT IS NULL): {active_records:,}")
    print(f"[{survey_id}_{layout_id}] Historical records (__END_AT IS NOT NULL): {historical_records:,}")
    print(f"[{survey_id}_{layout_id}] {'='*60}")
    
    return counter, total_inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: NEW - Single Survey Processing Function (for parallel execution)

# COMMAND ----------

def process_single_survey(survey_id, layout_id, json_path):
    """
    Process all files for a single survey/layout combination.
    This function is designed to be run in parallel.
    
    Returns:
        dict: Summary statistics for this survey/layout
    """
    result = {
        'survey_id': survey_id,
        'layout_id': layout_id,
        'files_found': 0,
        'files_skipped': 0,
        'files_processed': 0,
        'files_failed': 0,
        'error': None
    }
    
    try:
        # Get all JSON files for this survey/layout with metadata
        json_files = get_json_files(json_path, survey_id, layout_id)
        
        if not json_files:
            print(f"[{survey_id}_{layout_id}] No files found")
            return result
        
        result['files_found'] = len(json_files)
        
        # Get already processed files
        processed_files = get_processed_files(survey_id, layout_id)
        
        # Filter out already processed files
        files_to_process = [f for f in json_files if f['path'] not in processed_files]
        files_skipped = len(json_files) - len(files_to_process)
        result['files_skipped'] = files_skipped
        
        print(f"\n{'='*60}")
        print(f"[{survey_id}_{layout_id}] Processing survey/layout")
        print(f"[{survey_id}_{layout_id}] Found {len(json_files)} file(s)")
        print(f"[{survey_id}_{layout_id}] Already processed: {files_skipped} file(s)")
        print(f"[{survey_id}_{layout_id}] To process: {len(files_to_process)} file(s)")
        print(f"{'='*60}")
        
        if not files_to_process:
            print(f"[{survey_id}_{layout_id}] ✓ All files already processed. Skipping.")
            return result
        
        # Process each new file
        for file_info in files_to_process:
            file_path = file_info['path']
            file_size_mb = file_info['size_bytes'] / (1024 * 1024)
            
            print(f"\n[{survey_id}_{layout_id}] Processing file: {file_info['name']} ({file_size_mb:.2f} MB)")
            
            # Log processing start
            try:
                adapted_file_info = {
                    'file_path': file_info['path'],
                    'file_name': file_info['name'],
                    'file_size': file_info['size_bytes'],
                    'file_mtime': file_info['modification_time'],
                }
                log_file_processing_start(adapted_file_info, file_type='json')
            except Exception as e:
                print(f"[{survey_id}_{layout_id}] Warning: Could not log processing start: {e}")
            
            try:
                # Use streaming approach with FIXED SCD Type 2
                respondent_count, records_inserted = process_large_file_streaming(
                    file_path=file_path,
                    survey_id=survey_id,
                    layout_id=layout_id,
                    batch_size=5000,  # Adjust based on memory constraints
                    max_workers=3     # Adjust based on cluster resources
                )
                
                # Log successful completion
                log_file_processing_complete(
                    file_path=file_path,
                    status='SUCCESS',
                    metrics={
                        'records_read': respondent_count,
                        'records_inserted': records_inserted,
                    }
                )
                result['files_processed'] += 1
                print(f"[{survey_id}_{layout_id}] ✓ Successfully processed and logged: {file_info['name']}")
                
            except Exception as e:
                # Log failure
                error_message = str(e)[:1000]  # Truncate to avoid excessively long error messages
                log_file_processing_complete(
                    file_path=file_path,
                    status='FAILED',
                    error_info={'error_message': error_message}
                )
                result['files_failed'] += 1
                print(f"[{survey_id}_{layout_id}] ✗ Failed to process {file_path}: {e}")
                import traceback
                print(traceback.format_exc())
                
                # Continue to next file instead of stopping entire job
                continue
    
    except Exception as e:
        result['error'] = str(e)
        print(f"[{survey_id}_{layout_id}] ✗ Critical error processing survey: {e}")
        import traceback
        print(traceback.format_exc())
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: NEW - Main Parallel Processing Loop

# COMMAND ----------

# Initialize summary counters
total_files_found = 0
total_files_skipped = 0
total_files_processed = 0
total_files_failed = 0
survey_results = []

print(f"\n{'='*60}")
print(f"Starting PARALLEL processing of {len(surveys_and_layouts_ids_list)} survey/layout combinations")
print(f"Max parallel surveys: {MAX_PARALLEL_SURVEYS}")
print(f"{'='*60}\n")

# Process surveys in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SURVEYS) as executor:
    # Submit all survey/layout combinations for parallel processing
    future_to_survey = {
        executor.submit(process_single_survey, survey_id, layout_id, json_path): (survey_id, layout_id)
        for survey_id, layout_id in surveys_and_layouts_ids_list
    }
    
    # Collect results as they complete
    for future in as_completed(future_to_survey):
        survey_id, layout_id = future_to_survey[future]
        try:
            result = future.result()
            survey_results.append(result)
            
            # Update totals
            total_files_found += result['files_found']
            total_files_skipped += result['files_skipped']
            total_files_processed += result['files_processed']
            total_files_failed += result['files_failed']
            
            if result['error']:
                print(f"\n⚠ [{survey_id}_{layout_id}] Completed with error: {result['error']}")
            else:
                print(f"\n✓ [{survey_id}_{layout_id}] Completed successfully")
        
        except Exception as e:
            print(f"\n✗ [{survey_id}_{layout_id}] Failed with exception: {e}")
            import traceback
            print(traceback.format_exc())

# Print final summary
print(f"\n{'='*60}")
print("FINAL Processing Summary")
print(f"{'='*60}")
print(f"Total survey/layout combinations: {len(surveys_and_layouts_ids_list)}")
print(f"Total files found: {total_files_found}")
print(f"Files skipped (already processed): {total_files_skipped}")
print(f"Files successfully processed: {total_files_processed}")
print(f"Files failed: {total_files_failed}")
print(f"{'='*60}")

# Print per-survey summary
print(f"\nPer-Survey Summary:")
print(f"{'='*60}")
for result in survey_results:
    status = "✓" if not result['error'] and result['files_failed'] == 0 else "⚠" if result['files_failed'] > 0 else "✗"
    print(f"{status} {result['survey_id']}_{result['layout_id']}: "
          f"{result['files_processed']} processed, "
          f"{result['files_failed']} failed, "
          f"{result['files_skipped']} skipped")
print(f"{'='*60}")