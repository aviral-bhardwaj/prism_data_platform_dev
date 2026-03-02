"""
Logging utilities for file processing

This module contains the logging functions extracted from the file processing notebook.
Simply import these functions in your notebook to use them.

Usage in notebook:
    from logging_utils import log_file_processing_start, log_file_processing_complete
"""

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from delta.tables import DeltaTable


def log_file_processing_start(file_info, file_type):
    """
    Log the start of file processing with status='IN_PROGRESS'.
    Includes comprehensive metadata and execution context.
    """
    import hashlib

    # Extract survey_id and layout_id from filename
    parts = file_info['file_name'].replace('.csv', '').split('_')
    survey_id = parts[-3] if len(parts) >= 3 else None
    layout_id = parts[-2] if len(parts) >= 2 else None

    # Calculate file checksum
    # try:
    file_path_local = file_info['file_path'].replace('dbfs:', '')
    with open(file_path_local, 'rb') as f:
        file_checksum = hashlib.md5(f.read()).hexdigest()
    # except:
    #     file_checksum = None

    # Detect file encoding
    try:
        import chardet
        with open(file_path_local, 'rb') as f:
            raw_data = f.read(10000)  # Read first 10KB
            result = chardet.detect(raw_data)
            file_encoding = result['encoding']
    except:
        file_encoding = 'UTF-8'  # Default assumption

    # Get execution context
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        execution_type = 'NOTEBOOK'
    except:
        notebook_path = None
        execution_type = 'UNKNOWN'

    try:
        user = spark.sql("SELECT session_user()").collect()[0][0]
    except:
        user = None

    # Try to get job context if running in a job
    job_id = None
    run_id = None
    try:
        job_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get()
        run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().get()
        if job_id:
            execution_type = 'JOB'
    except:
        pass

    log_schema = StructType([
        # File identification
        StructField("file_path", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("survey_id", StringType(), True),
        StructField("layout_id", StringType(), True),

        # File metadata
        StructField("file_size_bytes", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
        StructField("file_checksum", StringType(), True),
        StructField("file_encoding", StringType(), True),
        StructField("file_row_count", LongType(), True),

        # Schema tracking
        StructField("incoming_schema", StringType(), True),
        StructField("schema_changes", StringType(), True),

        # Processing timing
        StructField("processing_start_time", TimestampType(), True),
        StructField("processing_end_time", TimestampType(), True),
        StructField("processing_duration_seconds", IntegerType(), True),

        # Status
        StructField("status", StringType(), True),

        # Record counts
        StructField("records_read", LongType(), True),
        StructField("records_after_dedup", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_unchanged", LongType(), True),
        StructField("duplicate_rows", LongType(), True),

        # Data quality
        StructField("null_key_columns", StringType(), True),

        # Error details
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_stacktrace", StringType(), True),
        StructField("failed_at_step", StringType(), True),

        # Execution context
        StructField("execution_type", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("job_run_id", StringType(), True),
        StructField("notebook_path", StringType(), True),
        StructField("executed_by_user", StringType(), True),

        # Legacy compatibility
        StructField("respondent_count", IntegerType(), True)
    ])

    log_data = [(
        file_info['file_path'],
        file_info['file_name'],
        survey_id,
        layout_id,
        file_info['file_size'],
        file_info['file_mtime'],
        file_checksum,
        file_encoding,
        None,  # file_row_count - will be set on completion
        None,  # incoming_schema - will be set on completion
        None,  # schema_changes - will be set on completion
        datetime.now(),
        None,  # processing_end_time
        None,  # processing_duration_seconds
        'IN_PROGRESS',
        None,  # records_read
        None,  # records_after_dedup
        None,  # records_inserted
        None,  # records_updated
        None,  # records_unchanged
        None,  # duplicate_rows
        None,  # null_key_columns
        None,  # error_type
        None,  # error_message
        None,  # error_stacktrace
        None,  # failed_at_step
        execution_type,
        job_id,
        run_id,
        notebook_path,
        user,
        None   # respondent_count
    )]

    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(FILE_LOG_TABLE)


def log_file_processing_complete(file_path, status, metrics=None, error_info=None):
    """
    Update the file processing log with completion status.
    Uses Delta MERGE to update the existing IN_PROGRESS record.

    Args:
        file_path: Path to the file
        status: 'SUCCESS' or 'FAILED'
        metrics: Dict with processing metrics
        error_info: Dict with error details
    """
    delta_table = DeltaTable.forName(spark, FILE_LOG_TABLE)

    # Get processing start time to calculate duration
    try:
        start_time_df = spark.sql(f"""
            SELECT processing_start_time
            FROM {FILE_LOG_TABLE}
            WHERE file_path = '{file_path}'
            AND status = 'IN_PROGRESS'
        """)
        start_time = start_time_df.collect()[0].processing_start_time if start_time_df.count() > 0 else None
    except:
        start_time = None

    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds()) if start_time else None

    # Extract metrics
    if metrics is None:
        metrics = {}

    file_row_count = metrics.get('file_row_count')
    records_read = metrics.get('records_read')
    records_after_dedup = metrics.get('records_after_dedup')
    records_inserted = metrics.get('records_inserted')
    records_updated = metrics.get('records_updated')
    records_unchanged = metrics.get('records_unchanged')
    duplicate_rows = metrics.get('duplicate_rows')
    null_key_columns = metrics.get('null_key_columns')
    incoming_schema = metrics.get('incoming_schema')
    schema_changes = metrics.get('schema_changes')

    # Extract error info
    if error_info is None:
        error_info = {}

    error_type = error_info.get('error_type')
    error_message = error_info.get('error_message')
    error_stacktrace = error_info.get('error_stacktrace')
    failed_at_step = error_info.get('failed_at_step')

    update_schema = StructType([
        StructField("file_path", StringType(), False),
        StructField("status", StringType(), True),
        StructField("processing_end_time", TimestampType(), True),
        StructField("processing_duration_seconds", IntegerType(), True),
        StructField("file_row_count", LongType(), True),
        StructField("incoming_schema", StringType(), True),
        StructField("schema_changes", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_after_dedup", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_unchanged", LongType(), True),
        StructField("duplicate_rows", LongType(), True),
        StructField("null_key_columns", StringType(), True),
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_stacktrace", StringType(), True),
        StructField("failed_at_step", StringType(), True)
    ])

    update_data = spark.createDataFrame(
        [(
            file_path,
            status,
            end_time,
            duration_seconds,
            file_row_count,
            incoming_schema,
            schema_changes,
            records_read,
            records_after_dedup,
            records_inserted,
            records_updated,
            records_unchanged,
            duplicate_rows,
            null_key_columns,
            error_type,
            error_message,
            error_stacktrace,
            failed_at_step
        )],
        schema=update_schema
    )

    delta_table.alias("target").merge(
        update_data.alias("source"),
        "target.file_path = source.file_path"
    ).whenMatchedUpdate(
        set={
            "status": "source.status",
            "processing_end_time": "source.processing_end_time",
            "processing_duration_seconds": "source.processing_duration_seconds",
            "file_row_count": "source.file_row_count",
            "incoming_schema": "source.incoming_schema",
            "schema_changes": "source.schema_changes",
            "records_read": "source.records_read",
            "records_after_dedup": "source.records_after_dedup",
            "records_inserted": "source.records_inserted",
            "records_updated": "source.records_updated",
            "records_unchanged": "source.records_unchanged",
            "duplicate_rows": "source.duplicate_rows",
            "null_key_columns": "source.null_key_columns",
            "error_type": "source.error_type",
            "error_message": "source.error_message",
            "error_stacktrace": "source.error_stacktrace",
            "failed_at_step": "source.failed_at_step",
            "respondent_count": "source.records_read"  # For backward compatibility
        }
    ).execute()
