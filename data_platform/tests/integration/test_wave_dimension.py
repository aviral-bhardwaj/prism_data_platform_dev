"""
Integration tests for the Silver layer dim_wave derive_wave_fields function.

Tests wave description parsing (e.g., '25Q1' -> year 2025, period 'Q1 2025').
"""

import sys
import os
import pytest
from pyspark.sql import functions as F

# Add source to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src", "silver_layer_scripts"))

from wave_utils import derive_wave_fields


class TestDeriveWaveFields:
    """Tests for wave description parsing and field derivation."""

    def test_quarterly_pattern(self, spark):
        """25Q4 -> val_year=2025, period='Q4 2025', period_alt='2025 Q4'."""
        df = spark.createDataFrame([("25Q4",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "Q4 2025"
        assert result["period_alt"] == "2025 Q4"

    def test_quarterly_lowercase(self, spark):
        """25q1 (lowercase) should parse identically to 25Q1."""
        df = spark.createDataFrame([("25q1",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "Q1 2025"

    def test_half_year_pattern_h(self, spark):
        """25H1 -> val_year=2025, period='H1 2025'."""
        df = spark.createDataFrame([("25H1",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "H1 2025"
        assert result["period_alt"] == "2025 H1"

    def test_half_year_pattern_s(self, spark):
        """25S2 -> val_year=2025, period='H2 2025' (s treated as h)."""
        df = spark.createDataFrame([("25s2",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "H2 2025"

    def test_four_digit_year(self, spark):
        """2025 -> val_year=2025, period='2025'."""
        df = spark.createDataFrame([("2025",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "2025"
        assert result["period_alt"] == "2025"

    def test_with_separator(self, spark):
        """25-Q4 (with dash separator) should still parse correctly."""
        df = spark.createDataFrame([("25-Q4",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "Q4 2025"

    def test_with_spaces(self, spark):
        """' 25Q4 ' (with spaces) should be trimmed and parsed."""
        df = spark.createDataFrame([("  25Q4  ",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] == 2025
        assert result["period"] == "Q4 2025"

    def test_unrecognized_pattern(self, spark):
        """Unrecognized patterns should produce None values."""
        df = spark.createDataFrame([("invalid",)], ["desc_wave"])
        result = derive_wave_fields(df, "desc_wave").collect()[0]

        assert result["val_year"] is None
        assert result["period"] is None
        assert result["period_alt"] is None

    def test_multiple_rows(self, spark):
        """Verify correct derivation across multiple rows in a single DataFrame."""
        data = [("25Q1",), ("25Q2",), ("25H1",), ("2025",)]
        df = spark.createDataFrame(data, ["desc_wave"])
        results = derive_wave_fields(df, "desc_wave").orderBy("desc_wave").collect()

        assert len(results) == 4
        # 2025 (four digit)
        assert results[0]["val_year"] == 2025
        # 25H1
        assert results[1]["period"] == "H1 2025"
        # 25Q1
        assert results[2]["period"] == "Q1 2025"
        # 25Q2
        assert results[3]["period"] == "Q2 2025"
