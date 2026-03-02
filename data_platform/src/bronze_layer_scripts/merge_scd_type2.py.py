# Databricks notebook source
from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, IntegerType
from pathlib import Path
import sys
import os
import json

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