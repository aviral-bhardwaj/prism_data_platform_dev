# Databricks notebook source
# MAGIC %pip install ijson polars

# COMMAND ----------

import dlt
import json
from datetime import datetime
from pyspark.sql import functions as F
from pathlib import Path
import polars as pl
import ijson
import os
from threading import Lock


# COMMAND ----------

# Configuration
spark.conf.set("spark.sql.caseSensitive", "true")
json_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_responses_endpoint/'
FILE_SIZE_THRESHOLD_MB = 500  # Parameterizable threshold in MB


# COMMAND ----------

surveys_df = spark.table("prism_bronze.data_plat_control.surveys")
surveys_and_layouts_ids_list = [(row.survey_id, row.layout_id) for row in surveys_df.collect()]

# COMMAND ----------

def get_json_files_with_sizes(base_path, survey_id, layout_id):
    """
    Get all JSON files and their sizes for a survey/layout combination
    Returns list of tuples: (file_path, size_in_mb)
    """
    dir_path = Path(base_path) / f"{survey_id}_{layout_id}"
    
    if not dir_path.exists():
        return []
    
    json_files = list(dir_path.glob("*.json"))
    
    files_with_sizes = []
    for file_path in json_files:
        size_mb = file_path.stat().st_size / (1024 * 1024)  # Convert to MB
        files_with_sizes.append((str(file_path), size_mb))
    
    return files_with_sizes

# COMMAND ----------

def process_large_file_streaming(file_path, survey_id, layout_id, batch_size=2000, max_workers=3):
    """
    Process large JSON file using streaming approach with parallel writes
    """
    from threading import Lock
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    melted_table_name = f'prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}_melted'

    current_timestamp = datetime.now()
    
    # Truncate existing melted table
    try:
        spark.sql(f"TRUNCATE TABLE {melted_table_name}")
        print(f"Truncated existing melted table: {melted_table_name}")
    except Exception as e:
        print(f"No existing melted table to truncate: {e}")
    
    # Create empty table with schema if it doesn't exist
    try:
        spark.sql(f"DESCRIBE TABLE {melted_table_name}")
    except Exception:
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        schema = StructType([
            StructField("survey_id", StringType(), True),
            StructField("layout_id", StringType(), True),
            StructField("uuid", StringType(), True),
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
            StructField("dte_create", TimestampType(), True),
            StructField("dte_update", TimestampType(), True)
        ])
        
        empty_df = spark.createDataFrame([], schema)
        empty_df.write.format("delta").mode("append").saveAsTable(melted_table_name)
        print(f"Created empty table with schema: {melted_table_name}")
    
    table_creation_lock = Lock()
    
    def write_to_delta(batch_data):
        """Write a batch to Delta table"""
        batch_num, df_batch, total_respondents = batch_data
        
        df_spark = spark.createDataFrame(df_batch.to_pandas())
        
        # Thread-safe table write
        with table_creation_lock:
            df_spark.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(melted_table_name)
        
        return batch_num, total_respondents
    
    counter = 0
    batch_num = 0
    melted_dfs = []
    write_tasks = []
    
    print(f"Processing large file with streaming: {file_path}")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        
        with open(file_path, 'rb') as f:
            for resp in ijson.items(f, 'item'):
                uuid = resp['uuid']
                
                df_long = pl.DataFrame({
                    'survey_id': [survey_id] * len(resp),
                    'layout_id': [layout_id] * len(resp),
                    'uuid': [uuid] * len(resp),
                    'key': list(resp.keys()),
                    'value': [str(v) for v in resp.values()],
                    'dte_create': [current_timestamp] * len(resp),
                    'dte_update': [current_timestamp] * len(resp)
                })
                
                melted_dfs.append(df_long)
                counter += 1
                
                if counter % batch_size == 0:
                    df_batch = pl.concat(melted_dfs)
                    batch_num += 1
                    
                    future = executor.submit(write_to_delta, (batch_num, df_batch, counter))
                    write_tasks.append(future)
                    
                    print(f"Batch {batch_num}: {counter} respondents processed, submitted for writing.")
                    melted_dfs = []
            
            # Process final batch
            if melted_dfs:
                df_batch = pl.concat(melted_dfs)
                batch_num += 1
                future = executor.submit(write_to_delta, (batch_num, df_batch, counter))
                write_tasks.append(future)
                print(f"Final batch {batch_num}: {counter} respondents submitted.")
        
        print(f"\nWaiting for all Delta writes to complete...")
        for future in as_completed(write_tasks):
            completed_batch, total_resp = future.result()
            print(f"✓ Batch {completed_batch} completed (up to {total_resp} respondents)")
    
    print(f"Processing completed: {counter} total respondents written to {melted_table_name}")

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", "true")

for survey_id, layout_id in surveys_and_layouts_ids_list:
    
    # Check file sizes to determine processing approach
    files_with_sizes = get_json_files_with_sizes(json_path, survey_id, layout_id)
    
    if not files_with_sizes:
        print(f"No files found for {survey_id}_{layout_id}")
        continue
    
    large_files = [(f, s) for f, s in files_with_sizes if s > FILE_SIZE_THRESHOLD_MB]
    small_files = [(f, s) for f, s in files_with_sizes if s <= FILE_SIZE_THRESHOLD_MB]
    
    # Process large files with streaming merge approach
    if large_files:
        print(f"Found {len(large_files)} large file(s) for {survey_id}_{layout_id}")
        for file_path, size_mb in large_files:
            print(f"Processing large file: {file_path} ({size_mb:.2f} MB)")
            process_large_file_streaming(file_path, survey_id, layout_id)
    
    # Process small files with standard DLT approach
    if small_files:
        print(f"Processing {len(small_files)} small file(s) for {survey_id}_{layout_id} with standard DLT")
        
        view_name = f"raw_survey_answers_{survey_id}_{layout_id}_json_stream"
        input_path = f"{json_path}/{survey_id}_{layout_id}/*.json"
        
        @dlt.view(name=view_name)
        def create_raw_survey_answers_json_stream_views(sid=survey_id, lid=layout_id, ipath=input_path):
            df = (
                spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("multiline", "true")
                    .option("cloudFiles.inferColumnTypes", "true")
                    .option("cloudFiles.partitionColumns", "")
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                    .option("cloudFiles.maxFilesPerTrigger", "1")
                    .load(ipath)
            )

            if "qsamp" in df.columns:
                df = df.withColumnRenamed("qsamp", "qsamp_1")

            key_cols = ["uuid"]
            non_key_cols = [c for c in df.columns if c not in key_cols]

            df = df.withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in non_key_cols]), 256))

            df = df \
                .withColumn("bronze_updated_at", F.current_timestamp()) \
                .withColumn("file_name", F.col("_metadata.file_path")) \
                .withColumn("survey_id", F.lit(sid)) \
                .withColumn("layout_id", F.lit(lid))

            return df

        dlt.create_streaming_table(
            f"prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}",
            comment=f"Raw survey responses for survey {survey_id} and layout {layout_id}"
        )

        dlt.apply_changes(
            target=f"prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}",
            source=view_name,
            keys=["uuid", "survey_id", "layout_id"],
            sequence_by="bronze_updated_at",
            track_history_column_list=["row_hash"],
            stored_as_scd_type=2,
        )