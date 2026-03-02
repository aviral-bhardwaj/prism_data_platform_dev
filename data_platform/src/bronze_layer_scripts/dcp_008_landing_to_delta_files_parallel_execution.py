# Databricks notebook source
# MAGIC %md
# MAGIC # Generic File Processing with SCD Type 2 and File Tracking
# MAGIC
# MAGIC This notebook processes CSV and JSON files with:
# MAGIC - **Batch processing** instead of streaming
# MAGIC - **SCD Type 2** for tracking historical changes
# MAGIC - **File Tracking** to avoid reprocessing already processed files
# MAGIC - **Full audit trail** of processing history
# MAGIC
# MAGIC ## Key Features:
# MAGIC 1. Delta table-based file processing log
# MAGIC 2. Automatic skip of already-processed files
# MAGIC 3. Retry capability for failed files
# MAGIC 4. No streaming checkpoints required
# MAGIC 5. Support for both CSV and JSON formats

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType
from pathlib import Path
import sys
import os
import json

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

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# Load configuration
with open('config.json', 'r') as f:
    config_data = json.load(f)

CATALOG = config_data['catalog']
SCHEMA = config_data['schema']
FILE_LOG_TABLE = config_data['file_log_table']
REPROCESS_FAILED = config_data['reprocess_failed']
FILE_CONFIGS = config_data['file_configs']

# COMMAND ----------



# Set the global variables in the logging_utils module
logging_utils.spark = spark
logging_utils.dbutils = dbutils
logging_utils.FILE_LOG_TABLE = FILE_LOG_TABLE

print("Logging utilities imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: File Discovery and Tracking Functions

# COMMAND ----------

def get_files_to_process(config, file_type):
    """
    Discover files and filter out already processed ones.
    Returns list of file_info dicts to process.
    """
    source_path = config["source_path"]
    file_pattern = config["file_pattern"]
    file_format = config["file_format"]
    
    print(f"Looking for files with pattern: {file_pattern}")
    print(f"Source path: {source_path}")
    print(f"File format: {file_format}")
    
    # List all files in the directory
    try:
        all_files = dbutils.fs.ls(source_path)
    except Exception as e:
        print(f"Error listing files: {e}")
        return []
    
    # Filter files based on format and pattern
    matching_files = []
    if file_format == "csv":
        # CSV pattern matching
        file_type_identifier = file_pattern.replace('survey_file_', '').replace('*.csv', '')
        for file_info in all_files:
            file_name = file_info.name
            if file_name.startswith(f'survey_file_{file_type_identifier}_') and file_name.endswith('.csv'):
                matching_files.append(file_info)
    elif file_format == "json":
        # JSON pattern matching (all .json files)
        for file_info in all_files:
            file_name = file_info.name
            if file_name.endswith('.json'):
                matching_files.append(file_info)
    
    print(f"Found {len(matching_files)} files matching pattern")
    
    if len(matching_files) == 0:
        return []
    
    # Get already processed files
    try:
        if REPROCESS_FAILED:
            processed_files_df = spark.sql(f"""
                SELECT file_path 
                FROM {FILE_LOG_TABLE}
                WHERE status = 'SUCCESS'
            """)
        else:
            processed_files_df = spark.sql(f"""
                SELECT file_path 
                FROM {FILE_LOG_TABLE}
                WHERE status IN ('SUCCESS', 'FAILED')
            """)
        
        processed_files = set([row.file_path for row in processed_files_df.collect()])
        print(f"Already processed: {len(processed_files)} files")
    except Exception as e:
        print(f"Note: Could not read log table (might be first run): {e}")
        processed_files = set()
    
    # Filter to unprocessed files and gather metadata
    files_to_process = []
    for file_info in matching_files:
        file_path = file_info.path
        
        if file_path not in processed_files:
            file_data = {
                'file_path': file_path,
                'file_name': file_info.name,
                'file_size': file_info.size,
                'file_mtime': datetime.fromtimestamp(file_info.modificationTime / 1000)
            }
            files_to_process.append(file_data)
    
    print(f"Files to process: {len(files_to_process)}")
    return files_to_process

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: File Reading Functions

# COMMAND ----------

def read_file(file_path, config, file_type):
    """
    Read a single file based on its format (CSV or JSON) and apply transformations.
    """
    file_format = config["file_format"]
    
    try:
        if file_format == "csv":
            # Read CSV file - ALL COLUMNS AS STRING
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("quote", "\"") \
                .option("escape", "\"") \
                .option("delimiter", ",") \
                .option("inferSchema", "false") \
                .load(file_path)
        
        elif file_format == "json":
            # Read JSON file
            json_multiline = config.get("json_multiline", True)
            df = spark.read.format("json") \
                .option("multiline", str(json_multiline).lower()) \
                .option("inferSchema", "true") \
                .option("mode", "PERMISSIVE") \
                .load(file_path)
            
            # DEBUG: Print what columns we actually got
            print(f"Columns found in {file_type}: {df.columns}")
            
            # Apply JSON-specific transformations
            json_transform = config.get("json_transform")
            if json_transform == "explode_variables":
                if "variables" in df.columns:
                    df = df.select(F.explode(F.col("variables")).alias("var")).select("var.*")
        
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Add standard audit columns
        df = df.withColumn("bronze_updated_at", F.lit(datetime.now()).cast("timestamp")) \
               .withColumn("file_name", F.lit(file_path))
        
        # Add ID extraction columns based on configuration
        id_extraction = config.get("id_extraction", "none")
        
        if id_extraction == "csv":
            # CSV pattern: extract survey_id (6 digits) and layout_id (5 digits) using regex
            df = df.withColumn("survey_id", F.regexp_extract(F.col("file_name"), r"(\d{6})", 1)) \
                   .withColumn("layout_id", F.regexp_extract(F.col("file_name"), r"_(\d{5})_", 1))
        
        elif id_extraction == "metadata":
            # Metadata pattern: extract survey_id and layout_id from filename
            filename_only = F.element_at(F.split(F.col("file_name"), "/"), -1)
            df = df.withColumn("survey_id", F.element_at(F.split(filename_only, "_"), -3)) \
                   .withColumn("layout_id", F.element_at(F.split(filename_only, "_"), -2))
        
        elif id_extraction == "layout":
            # Layout pattern: extract survey_id from filename
            filename_only = F.element_at(F.split(F.col("file_name"), "/"), -1)
            df = df.withColumn("survey_id", F.element_at(F.split(filename_only, "_"), -2))
        
        return df
    
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Table Creation and SCD Type 2 Logic

# COMMAND ----------

def create_or_get_table(table_name, comment):
    """
    Create target table if it doesn't exist, or return existing table.
    """
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    
    try:
        # Check if table exists
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        print(f"Table {full_table_name} already exists")
        return DeltaTable.forName(spark, full_table_name)
    except:
        print(f"Creating table {full_table_name}")
        # Create empty table with SCD Type 2 columns
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                __START_AT TIMESTAMP,
                __END_AT TIMESTAMP
            )
            USING DELTA
            COMMENT '{comment}'
        """)
        return DeltaTable.forName(spark, full_table_name)

# COMMAND ----------

def merge_scd_type2(source_df, catalog, schema, config):
    """
    Perform SCD Type 2 merge for a given dataframe.
    Returns dict with comprehensive metrics including schema tracking.
    """
    import json
    
    print(f"Processing table={config['table_name']}, keys={config['keys']}")

    if source_df.isEmpty():
        return {
            'file_row_count': 0,
            'records_read': 0,
            'records_after_dedup': 0,
            'records_inserted': 0,
            'records_updated': 0,
            'records_unchanged': 0,
            'duplicate_rows': 0,
            'null_key_columns': None,
            'incoming_schema': None,
            'schema_changes': None
        }

    target_table = f"{catalog}.{schema}.{config['table_name']}"
    keys = config["keys"]
    
    # Count original records
    records_read = source_df.count()
    file_row_count = records_read
    
    # Capture incoming schema
    incoming_schema_dict = {field.name: field.dataType.simpleString() for field in source_df.schema.fields}
    incoming_schema = json.dumps(incoming_schema_dict, indent=2)
    
    # Check for nulls in key columns
    null_counts = {}
    for key in keys:
        null_count = source_df.filter(F.col(key).isNull()).count()
        if null_count > 0:
            null_counts[key] = null_count
    
    null_key_columns = str(null_counts) if null_counts else None
    
    # Deduplicate
    incoming_df = source_df.dropDuplicates(keys)
    records_after_dedup = incoming_df.count()
    duplicate_rows = records_read - records_after_dedup

    # Compare columns (everything except keys + technical metadata)
    exclude = set(keys + ["bronze_updated_at", "file_name", "__START_AT", "__END_AT", "row_number"])
    compare_cols = [c for c in incoming_df.columns if c not in exclude]
    
    # Detect schema changes if target table exists
    schema_changes = None
    if spark.catalog.tableExists(target_table):
        target_df = spark.table(target_table)
        target_schema_dict = {field.name: field.dataType.simpleString() for field in target_df.schema.fields 
                             if field.name not in ['__START_AT', '__END_AT']}
        
        # Find differences
        changes = {}
        
        # New columns in source
        new_columns = set(incoming_schema_dict.keys()) - set(target_schema_dict.keys())
        if new_columns:
            changes['new_columns'] = list(new_columns)
        
        # Missing columns in source
        missing_columns = set(target_schema_dict.keys()) - set(incoming_schema_dict.keys())
        if missing_columns:
            changes['missing_columns'] = list(missing_columns)
        
        # Type changes
        type_changes = {}
        for col in set(incoming_schema_dict.keys()) & set(target_schema_dict.keys()):
            if incoming_schema_dict[col] != target_schema_dict[col]:
                type_changes[col] = {
                    'target': target_schema_dict[col],
                    'incoming': incoming_schema_dict[col]
                }
        if type_changes:
            changes['type_changes'] = type_changes
        
        if changes:
            schema_changes = json.dumps(changes, indent=2)
            print(f"⚠ Schema changes detected: {schema_changes}")

    # Create table if missing
    if not spark.catalog.tableExists(target_table):
        records_inserted = incoming_df.count()
        (incoming_df
            .withColumn("__START_AT", F.current_timestamp())
            .withColumn("__END_AT", F.lit(None).cast("timestamp"))
            .write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table))
        print(f"Initial load: {records_inserted} rows")
        return {
            'file_row_count': file_row_count,
            'records_read': records_read,
            'records_after_dedup': records_after_dedup,
            'records_inserted': records_inserted,
            'records_updated': 0,
            'records_unchanged': 0,
            'duplicate_rows': duplicate_rows,
            'null_key_columns': null_key_columns,
            'incoming_schema': incoming_schema,
            'schema_changes': None  # First load, no changes
        }

    target_delta = DeltaTable.forName(spark, target_table)
    
    # Count active records before merge
    active_before = spark.table(target_table).filter(F.col("__END_AT").isNull()).count()

    # Build scope condition for merge (survey_id/layout_id if available)
    scope_conditions = []
    if "survey_id" in incoming_df.columns:
        survey_ids = [row.survey_id for row in incoming_df.select("survey_id").distinct().collect()]
        survey_id_list = "','".join(str(sid) for sid in survey_ids)
        scope_conditions.append(f"target.survey_id IN ('{survey_id_list}')")
    if "layout_id" in incoming_df.columns:
        layout_ids = [row.layout_id for row in incoming_df.select("layout_id").distinct().collect()]
        layout_id_list = "','".join(str(lid) for lid in layout_ids)
        scope_conditions.append(f"target.layout_id IN ('{layout_id_list}')")
    
    # Merge condition: match on keys AND active records in scope
    key_conditions = [f"target.`{k}` <=> source.`{k}`" for k in keys]
    merge_key_match = " AND ".join(key_conditions)
    
    # Scope for the merge (include all active records in scope, not just matched keys)
    if scope_conditions:
        merge_condition = f"({merge_key_match}) AND target.__END_AT IS NULL AND " + " AND ".join(scope_conditions)
    else:
        merge_condition = f"{merge_key_match} AND target.__END_AT IS NULL"

    # Change detection
    change_conditions = [f"NOT (target.`{c}` <=> source.`{c}`)" for c in compare_cols]
    has_changed = " OR ".join(change_conditions) if change_conditions else "false"

    # Deletion condition (same scope, only active records)
    deletion_parts = ["target.__END_AT IS NULL"]
    if scope_conditions:
        deletion_parts.extend(scope_conditions)
    deletion_condition = " AND ".join(deletion_parts)
    
    (target_delta.alias("target")
        .merge(incoming_df.alias("source"), merge_condition)
        .whenMatchedUpdate(condition=has_changed, set={"__END_AT": F.current_timestamp()})
        .whenNotMatchedBySourceUpdate(condition=deletion_condition, set={"__END_AT": F.current_timestamp()})
        .execute())

    # Insert new active versions
    current_active = spark.table(target_table).filter(F.col("__END_AT").isNull())
    join_cond = [incoming_df[k] == current_active[k] for k in keys]
    records_to_insert = incoming_df.join(current_active, join_cond, "left_anti")
    records_inserted = records_to_insert.count()

    if records_inserted > 0:
        (records_to_insert
            .withColumn("__START_AT", F.current_timestamp())
            .withColumn("__END_AT", F.lit(None).cast("timestamp"))
            .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table))
    
    # Count active records after merge
    active_after = spark.table(target_table).filter(F.col("__END_AT").isNull()).count()
    
    # Calculate metrics
    records_updated = active_before - active_after + records_inserted  # Records that were closed
    records_unchanged = records_after_dedup - records_inserted

    print(f"Metrics: inserted={records_inserted}, updated={records_updated}, unchanged={records_unchanged}, duplicates={duplicate_rows}")
    
    return {
        'file_row_count': file_row_count,
        'records_read': records_read,
        'records_after_dedup': records_after_dedup,
        'records_inserted': records_inserted,
        'records_updated': records_updated,
        'records_unchanged': records_unchanged,
        'duplicate_rows': duplicate_rows,
        'null_key_columns': null_key_columns,
        'incoming_schema': incoming_schema,
        'schema_changes': schema_changes
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Main Processing Loop

# COMMAND ----------

def process_file_type(file_type, config):
    """
    Process all unprocessed files for a given file type.
    """
    print(f"\n{'='*60}")
    print(f"Processing file type: {file_type}")
    print(f"Table: {config['table_name']}")
    print(f"{'='*60}")
    
    # Get files to process
    files_to_process = get_files_to_process(config, file_type)
    
    if len(files_to_process) == 0:
        print(f"No new files to process for {file_type}")
        return {
            'file_type': file_type,
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'total_records_processed': 0,
            'total_records_inserted': 0
        }
    
    # Process each file
    files_processed = 0
    files_failed = 0
    total_records_processed = 0
    total_records_inserted = 0
    
    for file_info in files_to_process:
        print(f"\nProcessing: {file_info['file_name']}")
        
        # Log start
        log_file_processing_start(file_info, file_type)
        
        failed_at_step = None
        
        try:
            # Read the file (CSV or JSON)
            failed_at_step = 'read_file'
            df = read_file(file_info['file_path'], config, file_type)
            
            # Perform SCD Type 2 merge
            failed_at_step = 'scd_merge'
            metrics = merge_scd_type2(df, CATALOG, SCHEMA, config)
            
            # Log success
            log_file_processing_complete(
                file_path=file_info['file_path'],
                status='SUCCESS',
                metrics=metrics
            )
            
            files_processed += 1
            total_records_processed += metrics['records_read']
            total_records_inserted += metrics['records_inserted']
            print(f"✓ Success: {metrics['records_read']} records read, {metrics['records_inserted']} inserted")
            
        except Exception as e:
            # Capture full error details
            import traceback
            error_msg = str(e)
            error_trace = traceback.format_exc()
            error_type = type(e).__name__
            
            error_info = {
                'error_type': error_type,
                'error_message': error_msg,
                'error_stacktrace': error_trace,
                'failed_at_step': failed_at_step
            }
            
            log_file_processing_complete(
                file_path=file_info['file_path'],
                status='FAILED',
                error_info=error_info
            )
            
            files_failed += 1
            print(f"✗ Failed at step '{failed_at_step}': {error_msg}")
            print(error_trace)
    
    # Summary for this file type
    print(f"\n{file_type} Summary:")
    print(f"  Files found: {len(files_to_process)}")
    print(f"  Files processed successfully: {files_processed}")
    print(f"  Files failed: {files_failed}")
    print(f"  Total records processed: {total_records_processed}")
    print(f"  Total records inserted: {total_records_inserted}")
    
    return {
        'file_type': file_type,
        'files_found': len(files_to_process),
        'files_processed': files_processed,
        'files_failed': files_failed,
        'total_records_processed': total_records_processed,
        'total_records_inserted': total_records_inserted
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Execute Processing for All File Types

# COMMAND ----------

# Process all file types - WITH PARALLELISM
from concurrent.futures import ThreadPoolExecutor, as_completed

print(f"\n{'='*60}")
print("Starting batch processing with parallelism")
print(f"{'='*60}\n")

# Process file types in parallel
results = []
max_workers = 4  # Adjust based on your cluster size

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Submit all tasks
    future_to_file_type = {
        executor.submit(process_file_type, file_type, config): file_type 
        for file_type, config in FILE_CONFIGS.items()
    }
    
    # Collect results as they complete
    for future in as_completed(future_to_file_type):
        file_type = future_to_file_type[future]
        try:
            result = future.result()
            results.append(result)
        except Exception as exc:
            print(f"\n{file_type} generated an exception: {exc}")
            # Still add a result for summary
            results.append({
                'file_type': file_type,
                'files_found': 0,
                'files_processed': 0,
                'files_failed': 0,
                'total_records_processed': 0,
                'total_records_inserted': 0
            })

# Print final summary
print(f"\n{'='*60}")
print("FINAL Processing Summary")
print(f"{'='*60}")

total_files = sum(r['files_found'] for r in results)
total_success = sum(r['files_processed'] for r in results)
total_failed = sum(r['files_failed'] for r in results)
total_records = sum(r['total_records_processed'] for r in results)
total_inserted = sum(r['total_records_inserted'] for r in results)

print(f"Total file types: {len(FILE_CONFIGS)}")
print(f"Total files found: {total_files}")
print(f"Files successfully processed: {total_success}")
print(f"Files failed: {total_failed}")
print(f"Total records processed: {total_records}")
print(f"Total records inserted: {total_inserted}")
print(f"{'='*60}")

# Print per-file-type summary
print(f"\nPer-File-Type Summary:")
print(f"{'='*60}")
for result in results:
    status = "✓" if result['files_failed'] == 0 else "⚠"
    print(f"{status} {result['file_type']}: "
          f"{result['files_processed']} processed, "
          f"{result['files_failed']} failed, "
          f"{result['total_records_inserted']} records inserted")
print(f"{'='*60}")

