"""
Integration tests for the Bronze layer configuration management.

Tests config.json structure, data source definitions, and the
landing-to-delta pipeline table creation patterns.
"""

import json
import os
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, BooleanType,
)


CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "src", "bronze_layer_scripts", "config.json"
)


class TestBronzeConfig:
    """Tests for config.json integrity and structure."""

    def test_config_file_exists(self):
        """config.json must exist in bronze_layer_scripts."""
        assert os.path.isfile(CONFIG_PATH), f"config.json not found at {CONFIG_PATH}"

    def test_config_is_valid_json(self):
        """config.json must be valid JSON."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        assert isinstance(config, dict)

    def test_config_has_required_top_level_keys(self):
        """config.json must have catalog, schema, file_log_table, and file_configs."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        for key in ("catalog", "schema", "file_log_table", "file_configs"):
            assert key in config, f"Missing top-level key: {key}"

    def test_config_catalog_and_schema(self):
        """Catalog should be 'prism_bronze' and schema should be 'decipher'."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        assert config["catalog"] == "prism_bronze"
        assert config["schema"] == "decipher"

    def test_file_configs_have_required_fields(self):
        """Each file_config entry must have source_path, file_pattern, file_format, table_name, keys."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        required_fields = {"source_path", "file_pattern", "file_format", "table_name", "keys"}

        for name, fc in config["file_configs"].items():
            for field in required_fields:
                assert field in fc, f"File config '{name}' missing field '{field}'"

    def test_file_configs_keys_are_non_empty_lists(self):
        """Each file_config 'keys' field must be a non-empty list of strings."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        for name, fc in config["file_configs"].items():
            keys = fc["keys"]
            assert isinstance(keys, list), f"'{name}' keys must be a list"
            assert len(keys) > 0, f"'{name}' keys must be non-empty"
            for k in keys:
                assert isinstance(k, str), f"'{name}' key items must be strings"

    def test_expected_file_configs_present(self):
        """All expected data sources should be configured."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        expected = {
            "answers_mapping", "questions_mapping", "variable_mapping",
            "all_surveys", "survey_metadata", "survey_layouts",
        }
        actual = set(config["file_configs"].keys())

        assert expected.issubset(actual), f"Missing configs: {expected - actual}"

    def test_file_formats_are_valid(self):
        """File formats should be 'csv' or 'json'."""
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)

        valid_formats = {"csv", "json"}
        for name, fc in config["file_configs"].items():
            assert fc["file_format"] in valid_formats, (
                f"'{name}' has invalid file_format: {fc['file_format']}"
            )


class TestBronzeLandingToDelta:
    """Tests for landing-to-delta transformation patterns."""

    def test_json_autoloader_schema_inference(self, spark):
        """Simulate CloudFiles JSON ingestion and verify schema is inferred."""
        data = [
            ('{"path": "survey/1", "title": "Test Survey", "state": "live"}',),
            ('{"path": "survey/2", "title": "Another Survey", "state": "closed"}',),
        ]
        raw_df = spark.createDataFrame(data, ["json_str"])
        parsed = spark.read.json(raw_df.rdd.map(lambda r: r.json_str))

        assert "path" in parsed.columns
        assert "title" in parsed.columns
        assert "state" in parsed.columns
        assert parsed.count() == 2

    def test_csv_file_read_with_header(self, spark, tmp_path):
        """Verify CSV files can be read with headers matching expected schema."""
        csv_content = "id_survey_question,num_answer,survey_id,layout_id\nq1,1,101,201\nq2,0,101,201\n"
        csv_path = os.path.join(str(tmp_path), "test_answers.csv")
        with open(csv_path, "w") as f:
            f.write(csv_content)

        df = spark.read.option("header", "true").csv(csv_path)

        assert df.count() == 2
        assert set(df.columns) == {"id_survey_question", "num_answer", "survey_id", "layout_id"}

    def test_scd_type2_merge_pattern(self, spark, tmp_path):
        """Test SCD Type 2 merge: existing record updated, new record inserted."""
        delta_path = os.path.join(str(tmp_path), "delta_table")

        # Create initial target table
        target_data = [("survey/1", "Old Title", "2025-01-01")]
        target_df = spark.createDataFrame(target_data, ["path", "title", "loaded_at"])
        target_df.write.format("delta").save(delta_path)

        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, delta_path)

        # Source data with an update and a new record
        source_data = [
            ("survey/1", "Updated Title", "2025-02-01"),
            ("survey/2", "New Survey", "2025-02-01"),
        ]
        source_df = spark.createDataFrame(source_data, ["path", "title", "loaded_at"])

        # Perform merge
        delta_table.alias("target").merge(
            source_df.alias("source"),
            "target.path = source.path",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        result = spark.read.format("delta").load(delta_path)

        assert result.count() == 2
        updated = result.filter(F.col("path") == "survey/1").collect()[0]
        assert updated["title"] == "Updated Title"

        new_row = result.filter(F.col("path") == "survey/2").collect()[0]
        assert new_row["title"] == "New Survey"
