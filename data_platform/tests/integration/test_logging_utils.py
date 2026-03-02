"""
Integration tests for the file processing logging utilities.

Tests the logging schema structure and Delta table merge patterns
used by logging_utils.py. Actual Databricks context (dbutils, spark globals)
is not available, so tests focus on schema validation and merge logic.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, LongType,
)
from delta.tables import DeltaTable
from datetime import datetime


# The expected schema from logging_utils.py
LOG_SCHEMA = StructType([
    StructField("file_path", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("survey_id", StringType(), True),
    StructField("layout_id", StringType(), True),
    StructField("file_size_bytes", LongType(), True),
    StructField("file_modification_time", TimestampType(), True),
    StructField("file_checksum", StringType(), True),
    StructField("file_encoding", StringType(), True),
    StructField("file_row_count", LongType(), True),
    StructField("incoming_schema", StringType(), True),
    StructField("schema_changes", StringType(), True),
    StructField("processing_start_time", TimestampType(), True),
    StructField("processing_end_time", TimestampType(), True),
    StructField("processing_duration_seconds", IntegerType(), True),
    StructField("status", StringType(), True),
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
    StructField("failed_at_step", StringType(), True),
    StructField("execution_type", StringType(), True),
    StructField("job_id", StringType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("notebook_path", StringType(), True),
    StructField("executed_by_user", StringType(), True),
    StructField("respondent_count", IntegerType(), True),
])


class TestLogSchema:
    """Tests for the file processing log schema."""

    def test_schema_has_expected_field_count(self):
        """Log schema should have 32 fields."""
        assert len(LOG_SCHEMA.fields) == 32

    def test_file_path_is_required(self):
        """file_path should be non-nullable (primary key)."""
        field = next(f for f in LOG_SCHEMA.fields if f.name == "file_path")
        assert field.nullable is False

    def test_status_field_exists(self):
        """status field must exist for IN_PROGRESS / SUCCESS / FAILED tracking."""
        field_names = [f.name for f in LOG_SCHEMA.fields]
        assert "status" in field_names

    def test_error_fields_exist(self):
        """Error tracking fields must be present."""
        field_names = [f.name for f in LOG_SCHEMA.fields]
        for name in ("error_type", "error_message", "error_stacktrace", "failed_at_step"):
            assert name in field_names

    def test_record_count_fields_exist(self):
        """Record count tracking fields must be present."""
        field_names = [f.name for f in LOG_SCHEMA.fields]
        for name in ("records_read", "records_after_dedup", "records_inserted", "records_updated", "records_unchanged"):
            assert name in field_names


class TestLogDeltaMergePattern:
    """Tests for the Delta MERGE pattern used in log_file_processing_complete."""

    def test_merge_updates_in_progress_to_success(self, spark, tmp_path):
        """MERGE should update IN_PROGRESS record to SUCCESS with metrics."""
        delta_path = str(tmp_path / "log_table")
        now = datetime.now()

        # Create initial log entry (IN_PROGRESS)
        initial_data = [(
            "/path/to/file.csv", "file.csv", "101", "201",
            1024, now, "abc123", "UTF-8",
            None, None, None,
            now, None, None,
            "IN_PROGRESS",
            None, None, None, None, None, None, None,
            None, None, None, None,
            "NOTEBOOK", None, None, "/notebook/path", "user@example.com",
            None,
        )]
        log_df = spark.createDataFrame(initial_data, schema=LOG_SCHEMA)
        log_df.write.format("delta").save(delta_path)

        # Create update data
        update_schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("status", StringType(), True),
            StructField("processing_end_time", TimestampType(), True),
            StructField("processing_duration_seconds", IntegerType(), True),
            StructField("records_read", LongType(), True),
        ])
        update_data = spark.createDataFrame(
            [("/path/to/file.csv", "SUCCESS", now, 120, 500)],
            schema=update_schema,
        )

        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            update_data.alias("source"),
            "target.file_path = source.file_path",
        ).whenMatchedUpdate(set={
            "status": "source.status",
            "processing_end_time": "source.processing_end_time",
            "processing_duration_seconds": "source.processing_duration_seconds",
            "records_read": "source.records_read",
        }).execute()

        result = spark.read.format("delta").load(delta_path).collect()[0]
        assert result["status"] == "SUCCESS"
        assert result["records_read"] == 500
        assert result["processing_duration_seconds"] == 120

    def test_merge_updates_in_progress_to_failed(self, spark, tmp_path):
        """MERGE should update IN_PROGRESS record to FAILED with error info."""
        delta_path = str(tmp_path / "log_table_fail")
        now = datetime.now()

        initial_data = [(
            "/path/to/bad_file.csv", "bad_file.csv", "101", "201",
            512, now, "def456", "UTF-8",
            None, None, None,
            now, None, None,
            "IN_PROGRESS",
            None, None, None, None, None, None, None,
            None, None, None, None,
            "JOB", "job-1", "run-1", "/notebook/path", "user@example.com",
            None,
        )]
        log_df = spark.createDataFrame(initial_data, schema=LOG_SCHEMA)
        log_df.write.format("delta").save(delta_path)

        update_schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("status", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("failed_at_step", StringType(), True),
        ])
        update_data = spark.createDataFrame(
            [("/path/to/bad_file.csv", "FAILED", "ValueError", "Invalid schema", "schema_validation")],
            schema=update_schema,
        )

        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target").merge(
            update_data.alias("source"),
            "target.file_path = source.file_path",
        ).whenMatchedUpdate(set={
            "status": "source.status",
            "error_type": "source.error_type",
            "error_message": "source.error_message",
            "failed_at_step": "source.failed_at_step",
        }).execute()

        result = spark.read.format("delta").load(delta_path).collect()[0]
        assert result["status"] == "FAILED"
        assert result["error_type"] == "ValueError"
        assert result["failed_at_step"] == "schema_validation"


class TestFilenameParser:
    """Tests for survey_id/layout_id extraction from filenames."""

    @pytest.mark.parametrize("filename,expected_survey,expected_layout", [
        ("survey_file_Answers_mapping_101_201_v1.csv", "101", "201"),
        ("survey_file_Questions_mapping_102_202_v2.csv", "102", "202"),
        ("data_101_201_v1.csv", "101", "201"),
    ])
    def test_filename_id_extraction(self, filename, expected_survey, expected_layout):
        """Survey ID and layout ID should be extractable from filename parts."""
        parts = filename.replace(".csv", "").split("_")
        survey_id = parts[-3] if len(parts) >= 3 else None
        layout_id = parts[-2] if len(parts) >= 2 else None

        assert survey_id == expected_survey
        assert layout_id == expected_layout
