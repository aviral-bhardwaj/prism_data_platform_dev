# Databricks notebook source
# MAGIC %md
# MAGIC # Survey Response Processing with SCD Type 2 and File Tracking
# MAGIC
# MAGIC This notebook processes survey response JSON files with:
# MAGIC - **SCD Type 2** for tracking historical changes
# MAGIC - **File Tracking** to avoid reprocessing already processed files
# MAGIC - **Memory-efficient streaming** using ijson and batching
# MAGIC
# MAGIC ## Key Improvements:
# MAGIC 1. Delta table-based file processing log
# MAGIC 2. Automatic skip of already-processed files
# MAGIC 3. Retry capability for failed files
# MAGIC 4. Full audit trail of processing history

# COMMAND ----------

# MAGIC %pip install ijson polars

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from pathlib import Path
import polars as pl
import ijson
import os
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType

# COMMAND ----------

# Configuration
json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_responses_endpoint/'

# File tracking configuration
FILE_LOG_TABLE = "prism_bronze.data_plat_control.file_processing_log"
REPROCESS_FAILED = True  # Set to True to retry failed files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create File Processing Log Table
# MAGIC
# MAGIC This table tracks all processed files and their status.

# COMMAND ----------

def create_file_log_table():
    """
    Create the file processing log table if it doesn't exist.
    This table tracks which files have been processed.
    """
    schema = StructType([
        StructField("file_path", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("survey_id", StringType(), True),
        StructField("layout_id", StringType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
        StructField("processing_start_time", TimestampType(), True),
        StructField("processing_end_time", TimestampType(), True),
        StructField("status", StringType(), True),  # 'SUCCESS', 'FAILED', 'IN_PROGRESS'
        StructField("respondent_count", IntegerType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    try:
        spark.sql(f"DESCRIBE TABLE {FILE_LOG_TABLE}")
        print(f"✓ File log table already exists: {FILE_LOG_TABLE}")
    except Exception:
        # Create empty table
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(FILE_LOG_TABLE)
        print(f"✓ Created file log table: {FILE_LOG_TABLE}")

# Create the log table
create_file_log_table()

# COMMAND ----------

surveys_df = spark.table("prism_bronze.data_plat_control.surveys")
surveys_and_layouts_ids_list = [(row.survey_id, row.layout_id) for row in surveys_df.collect()]


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

def log_file_processing_start(file_info, survey_id, layout_id):
    """
    Log the start of file processing with status='IN_PROGRESS'.
    FIXED: Uses explicit schema to handle None values correctly.
    """
    # Define explicit schema to avoid 'CANNOT_DETERMINE_TYPE' error
    log_schema = StructType([
        StructField("file_path", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("survey_id", StringType(), True),
        StructField("layout_id", StringType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
        StructField("processing_start_time", TimestampType(), True),
        StructField("processing_end_time", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("respondent_count", IntegerType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    log_data = [(
        file_info['path'],
        file_info['name'],
        survey_id,
        layout_id,
        file_info['size_bytes'],
        file_info['modification_time'],
        datetime.now(),
        None,  # processing_end_time
        'IN_PROGRESS',
        None,  # respondent_count
        None,  # records_inserted
        None   # error_message
    )]
    
    # Create DataFrame with explicit schema
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.write.format("delta").mode("append").saveAsTable(FILE_LOG_TABLE)

# COMMAND ----------

def log_file_processing_complete(file_path, status, respondent_count='', records_inserted='', error_message=''):
    """
    Update the file processing log with completion status.
    Uses Delta MERGE to update the existing IN_PROGRESS record.
    """
    delta_table = DeltaTable.forName(spark, FILE_LOG_TABLE)
    
    update_data = spark.createDataFrame(
        [(file_path, status, datetime.now(), respondent_count, records_inserted, error_message)],
        ["file_path", "status", "processing_end_time", "respondent_count", "records_inserted", "error_message"]
    )
    
    delta_table.alias("target").merge(
        update_data.alias("source"),
        "target.file_path = source.file_path"
    ).whenMatchedUpdate(
        set={
            "status": "source.status",
            "processing_end_time": "source.processing_end_time",
            "respondent_count": "source.respondent_count",
            "records_inserted": "source.records_inserted",
            "error_message": "source.error_message"
        }
    ).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: File Processing Function (Unchanged Core Logic)

# COMMAND ----------

def process_large_file_streaming(file_path, survey_id, layout_id, batch_size=2000, max_workers=3):
    """
    Process JSON file of ANY size using streaming approach with FIXED SCD Type 2 implementation.
    Uses ijson for memory-efficient streaming and Delta merge for SCD Type 2 updates.
    
    SCD Type 2 Logic (CORRECTED):
    - Business Key: (survey_id, layout_id, uuid, key)
    - Tracks changes with __START_AT and __END_AT
    - Only inserts records that are NEW or CHANGED (not duplicates)
    
    Args:
        file_path: Path to the JSON file
        survey_id: Survey identifier
        layout_id: Layout identifier
        batch_size: Number of respondents per batch (default: 2000)
        max_workers: Maximum parallel write threads (default: 3)
    
    Returns:
        tuple: (respondent_count, total_records_inserted)
    """
    from threading import Lock
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from delta.tables import DeltaTable
    
    melted_table_name = f'prism_bronze.decipher_raw_answers_long_format.survey_responses_{survey_id}_{layout_id}'
    current_timestamp = datetime.now()
    
    # Define schema for Delta table with SCD Type 2 columns
    schema = StructType([
        StructField("survey_id", StringType(), True),
        StructField("layout_id", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("dte_create", TimestampType(), True),
        StructField("dte_update", TimestampType(), True),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True)
    ])
    
    # Create or verify table exists
    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {melted_table_name}")
        table_exists = True
        print(f"Using existing Delta table: {melted_table_name}")
    except Exception:
        # Create empty table with schema
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("overwrite").saveAsTable(melted_table_name)
        print(f"Created new Delta table: {melted_table_name}")
    
    table_creation_lock = Lock()
    
    def perform_scd2_merge(df_new):
        """
        FIXED SCD Type 2 implementation:
        Step 1: Close records that changed (set __END_AT)
        Step 2: Insert ONLY new or changed records (not duplicates!)
        """
        delta_table = DeltaTable.forName(spark, melted_table_name)
        
        # Get current active records from target BEFORE any updates
        active_before_merge = spark.table(melted_table_name) \
            .filter(F.col("__END_AT").isNull()) \
            .select(
                F.col("survey_id"),
                F.col("layout_id"),
                F.col("uuid"),
                F.col("key"),
                F.col("value").alias("existing_value"),
                F.col("dte_update").alias("existing_dte_update")
            )
        
        # STEP 1: Close records that have changed
        delta_table.alias("target").merge(
            df_new.alias("source"),
            """
            target.survey_id = source.survey_id AND
            target.layout_id = source.layout_id AND
            target.uuid = source.uuid AND
            target.key = source.key AND
            target.__END_AT IS NULL
            """
        ).whenMatchedUpdate(
            condition="target.value != source.value OR target.dte_update != source.dte_update",
            set={"__END_AT": "source.__START_AT"}
        ).execute()
        
        # STEP 2: Identify records to insert (NEW or CHANGED only, no duplicates)
        # Join with snapshot taken BEFORE the merge
        records_to_insert = df_new.alias("new").join(
            active_before_merge.alias("existing"),
            on=["survey_id", "layout_id", "uuid", "key"],
            how="left"
        ).filter(
            # NEW records: business key doesn't exist in target
            F.col("existing.existing_value").isNull() |
            # CHANGED records: business key exists but value or dte_update is different
            (
                (F.col("new.value") != F.col("existing.existing_value")) |
                (F.col("new.dte_update") != F.col("existing.existing_dte_update"))
            )
        ).select(
            F.col("new.survey_id"),
            F.col("new.layout_id"),
            F.col("new.uuid"),
            F.col("new.key"),
            F.col("new.value"),
            F.col("new.dte_create"),
            F.col("new.dte_update"),
            F.col("new.__START_AT"),
            F.col("new.__END_AT")
        )
        
        # Insert only new/changed records
        records_count = records_to_insert.count()
        if records_count > 0:
            records_to_insert.write.format("delta").mode("append").saveAsTable(melted_table_name)
            return records_count
        return 0
    
    def write_to_delta(batch_data):
        """Write a batch to Delta table using SCD Type 2 merge"""
        batch_num, df_batch, total_respondents = batch_data
        
        try:
            # Convert Polars DataFrame to Spark DataFrame
            df_spark = spark.createDataFrame(df_batch.to_pandas())
            
            # Add SCD Type 2 columns
            df_spark = df_spark \
                .withColumn("__START_AT", F.lit(current_timestamp)) \
                .withColumn("__END_AT", F.lit(None).cast(TimestampType()))
            
            # Thread-safe table merge with FIXED SCD Type 2 logic
            with table_creation_lock:
                inserted_count = perform_scd2_merge(df_spark)
            
            return batch_num, total_respondents, inserted_count, None
        except Exception as e:
            import traceback
            return batch_num, total_respondents, 0, f"{str(e)}\n{traceback.format_exc()}"
    
    counter = 0
    batch_num = 0
    melted_dfs = []
    write_tasks = []
    
    print(f"Processing file with streaming approach (SCD Type 2): {file_path}")
    print(f"Batch size: {batch_size} respondents, Max workers: {max_workers}")
    print(f"Business Keys: survey_id, layout_id, uuid, key")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        # Stream JSON file using ijson for memory efficiency
        with open(file_path, 'rb') as f:
            try:
                for resp in ijson.items(f, 'item'):
                    uuid = resp.get('uuid', '')
                    
                    # Convert response to long format
                    df_long = pl.DataFrame({
                        'survey_id': [survey_id] * len(resp),
                        'layout_id': [layout_id] * len(resp),
                        'uuid': [uuid] * len(resp),
                        'key': list(resp.keys()),
                        'value': [str(v) for v in resp.values()],
                        'dte_create': [current_timestamp] * len(resp),
                        'dte_update': [current_timestamp] * len(resp)
                    }).filter(pl.col('key')!='uuid')
                    
                    melted_dfs.append(df_long)
                    counter += 1
                    
                    # Write batch when size threshold reached
                    if counter % batch_size == 0:
                        df_batch = pl.concat(melted_dfs)
                        batch_num += 1
                        
                        future = executor.submit(write_to_delta, (batch_num, df_batch, counter))
                        write_tasks.append(future)
                        
                        print(f"Batch {batch_num}: {counter} respondents processed, submitted for SCD2 merge.")
                        melted_dfs = []  # Clear memory
                
                # Process final batch if any records remain
                if melted_dfs:
                    df_batch = pl.concat(melted_dfs)
                    batch_num += 1
                    future = executor.submit(write_to_delta, (batch_num, df_batch, counter))
                    write_tasks.append(future)
                    print(f"Final batch {batch_num}: {counter} respondents submitted.")
            
            except Exception as e:
                print(f"Error during streaming: {e}")
                import traceback
                print(traceback.format_exc())
                raise
        
        # Wait for all writes to complete
        print(f"\nWaiting for all Delta SCD2 merges to complete...")
        errors = []
        total_inserted = 0
        for future in as_completed(write_tasks):
            completed_batch, total_resp, inserted_count, error = future.result()
            if error:
                errors.append(f"Batch {completed_batch}: {error}")
                print(f"✗ Batch {completed_batch} failed: {error}")
            else:
                total_inserted += inserted_count
                print(f"✓ Batch {completed_batch} completed (up to {total_resp} respondents, {inserted_count} records inserted)")
        
        if errors:
            print(f"\n⚠ {len(errors)} batch(es) encountered errors:")
            for error in errors:
                print(f"  - {error}")
            raise Exception(f"File processing failed with {len(errors)} batch errors")
    
    # Show SCD Type 2 statistics
    total_records = spark.table(melted_table_name).count()
    active_records = spark.table(melted_table_name).filter(F.col("__END_AT").isNull()).count()
    historical_records = total_records - active_records
    
    print(f"\n{'='*60}")
    print(f"Processing completed: {counter} respondents processed")
    print(f"Table: {melted_table_name}")
    print(f"Total records inserted this run: {total_inserted}")
    print(f"Total records in table (including history): {total_records}")
    print(f"Active records (__END_AT IS NULL): {active_records}")
    print(f"Historical records (__END_AT IS NOT NULL): {historical_records}")
    print(f"{'='*60}")
    
    return counter, total_inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Main Processing Loop with File Tracking

# COMMAND ----------

# Track overall statistics
total_files_found = 0
total_files_skipped = 0
total_files_processed = 0
total_files_failed = 0

for survey_id, layout_id in surveys_and_layouts_ids_list:
    
    # Get all JSON files for this survey/layout with metadata
    json_files = get_json_files(json_path, survey_id, layout_id)
    
    if not json_files:
        print(f"No files found for {survey_id}_{layout_id}")
        continue
    
    total_files_found += len(json_files)
    
    # Get already processed files
    processed_files = get_processed_files(survey_id, layout_id)
    
    # Filter out already processed files
    files_to_process = [f for f in json_files if f['path'] not in processed_files]
    files_skipped = len(json_files) - len(files_to_process)
    total_files_skipped += files_skipped
    
    print(f"\n{'='*60}")
    print(f"Processing {survey_id}_{layout_id}")
    print(f"Found {len(json_files)} file(s)")
    print(f"Already processed: {files_skipped} file(s)")
    print(f"To process: {len(files_to_process)} file(s)")
    print(f"{'='*60}")
    
    if not files_to_process:
        print(f"✓ All files already processed for {survey_id}_{layout_id}. Skipping.")
        continue
    
    # Process each new file
    for file_info in files_to_process:
        file_path = file_info['path']
        file_size_mb = file_info['size_bytes'] / (1024 * 1024)
        
        print(f"\nProcessing file: {file_info['name']} ({file_size_mb:.2f} MB)")
        
        # Log processing start
        try:
            log_file_processing_start(file_info, survey_id, layout_id)
        except Exception as e:
            print(f"Warning: Could not log processing start: {e}")
        
        try:
            # Use streaming approach with FIXED SCD Type 2
            respondent_count, records_inserted = process_large_file_streaming(
                file_path=file_path,
                survey_id=survey_id,
                layout_id=layout_id,
                batch_size=2000,  # Adjust based on memory constraints
                max_workers=3     # Adjust based on cluster resources
            )
            
            # Log successful completion
            log_file_processing_complete(
                file_path=file_path,
                status='SUCCESS',
                respondent_count=respondent_count,
                records_inserted=records_inserted,
                error_message=''
            )
            total_files_processed += 1
            print(f"✓ Successfully processed and logged: {file_info['name']}")
            
        except Exception as e:
            # Log failure
            error_message = str(e)[:1000]  # Truncate to avoid excessively long error messages
            log_file_processing_complete(
                file_path=file_path,
                status='FAILED',
                error_message=error_message
            )
            total_files_failed += 1
            print(f"✗ Failed to process {file_path}: {e}")
            import traceback
            print(traceback.format_exc())
            
            # Continue to next file instead of stopping entire job
            continue

print(f"\n{'='*60}")
print("Processing Summary")
print(f"{'='*60}")
print(f"Total files found: {total_files_found}")
print(f"Files skipped (already processed): {total_files_skipped}")
print(f"Files successfully processed: {total_files_processed}")
print(f"Files failed: {total_files_failed}")
print(f"{'='*60}")