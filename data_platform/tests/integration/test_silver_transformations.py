"""
Integration tests for Silver layer transformations.

Tests tag parsing/pivoting, dimension table generation, and data
quality filtering patterns used across the Silver layer.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class TestTagParsingAndPivot:
    """Tests for Decipher survey tag parsing (key:value split and pivot)."""

    def test_tag_explode_and_split(self, spark, sample_survey_data):
        """Tags should be exploded and split into key/value columns."""
        df = sample_survey_data.select("title", F.explode("tags").alias("tag"))
        df = df.withColumn("tag_key", F.split("tag", ":")[0])
        df = df.withColumn("tag_value", F.split("tag", ":")[1])

        # Each survey has 4 tags, 3 surveys = 12 rows after explode
        assert df.count() == 12

        keys = {row["tag_key"] for row in df.select("tag_key").distinct().collect()}
        assert keys == {"nam_instrument", "nam_country", "wave", "instrument_detail"}

    def test_pivot_creates_columns_from_tags(self, spark, sample_survey_data):
        """Pivoting on tag_key should create one column per distinct key."""
        df = sample_survey_data.select(
            "title",
            F.explode("tags").alias("tag"),
        )
        df = (
            df.withColumn("tag_key", F.split("tag", ":")[0])
            .withColumn("tag_value", F.split("tag", ":")[1])
        )

        pivoted = df.groupBy("title").pivot("tag_key").agg(F.first("tag_value"))

        assert "nam_instrument" in pivoted.columns
        assert "nam_country" in pivoted.columns
        assert "wave" in pivoted.columns
        assert "instrument_detail" in pivoted.columns
        assert pivoted.count() == 3

    def test_pivot_values_are_correct(self, spark, sample_survey_data):
        """Pivoted values should match original tag values."""
        df = sample_survey_data.select(
            "title",
            F.explode("tags").alias("tag"),
        )
        df = (
            df.withColumn("tag_key", F.split("tag", ":")[0])
            .withColumn("tag_value", F.split("tag", ":")[1])
        )

        pivoted = df.groupBy("title").pivot("tag_key").agg(F.first("tag_value"))
        row = pivoted.filter(F.col("title") == "US Hotels 25Q1").collect()[0]

        assert row["nam_instrument"] == "US Hotels"
        assert row["nam_country"] == "US"
        assert row["wave"] == "25Q1"


class TestDimensionTablePatterns:
    """Tests for dimension table creation patterns."""

    def test_dim_instrument_join(self, spark):
        """Dimension join on (nam_instrument, nam_country) should produce correct gid_instrument."""
        dim_instrument = spark.createDataFrame(
            [(1, "US Hotels", "US"), (2, "Brazil NPS", "BR")],
            ["gid_instrument", "nam_instrument", "nam_country"],
        )
        fact_data = spark.createDataFrame(
            [("US Hotels", "US", "25Q1"), ("Brazil NPS", "BR", "2025")],
            ["nam_instrument", "nam_country", "wave"],
        )

        joined = fact_data.join(dim_instrument, on=["nam_instrument", "nam_country"], how="inner")

        assert joined.count() == 2
        us_row = joined.filter(F.col("wave") == "25Q1").collect()[0]
        assert us_row["gid_instrument"] == 1

    def test_drop_duplicates(self, spark):
        """Drop duplicates should remove exact duplicate rows."""
        data = [
            (1, "US Hotels", "25Q1"),
            (1, "US Hotels", "25Q1"),
            (2, "Brazil NPS", "2025"),
        ]
        df = spark.createDataFrame(data, ["gid_instrument", "nam_instrument", "wave"])
        deduped = df.drop_duplicates()

        assert deduped.count() == 2


class TestDataQualityFilters:
    """Tests for data quality filtering patterns used across Silver/Gold layers."""

    def test_filter_partial_respondents(self, spark, sample_respondent_data):
        """Partial respondents should be filtered out."""
        filtered = sample_respondent_data.filter(
            F.col("qualified_status") != "Partial"
        )
        assert filtered.count() == 4  # 5 total - 1 partial

    def test_filter_quality_start(self, spark, sample_respondent_data):
        """Only respondents with quality_start='Yes' should be kept."""
        filtered = sample_respondent_data.filter(
            F.col("quality_start") == "Yes"
        )
        assert filtered.count() == 4  # 5 total - 1 with No

    def test_combined_quality_filter(self, spark, sample_respondent_data):
        """Combined filter: non-partial AND quality_start=Yes."""
        filtered = sample_respondent_data.filter(
            (F.col("qualified_status") != "Partial") & (F.col("quality_start") == "Yes")
        )
        assert filtered.count() == 3

    def test_multiple_choice_filter(self, spark, sample_question_data):
        """Multiple choice questions with num_answer=0 should be excluded."""
        filtered = sample_question_data.filter(
            (F.col("typ_question") != "multiple") | (F.col("num_answer") != 0)
        )
        # q1 (single, ok), q2 (multiple num=0, filtered), q3 (multiple num=1, ok),
        # q4 (open, ok), q5 (single, ok)
        assert filtered.count() == 4


class TestDeltaTableOperations:
    """Tests for Delta table read/write patterns used in Silver layer."""

    def test_write_and_read_delta(self, spark, tmp_path, sample_question_data):
        """Data written to Delta format should be readable with full fidelity."""
        delta_path = str(tmp_path / "dim_question")
        sample_question_data.write.format("delta").save(delta_path)

        read_back = spark.read.format("delta").load(delta_path)

        assert read_back.count() == sample_question_data.count()
        assert set(read_back.columns) == set(sample_question_data.columns)

    def test_append_mode_delta(self, spark, tmp_path):
        """Append mode should add new records without overwriting existing ones."""
        delta_path = str(tmp_path / "append_test")

        batch1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        batch1.write.format("delta").save(delta_path)

        batch2 = spark.createDataFrame([(3, "c"), (4, "d")], ["id", "val"])
        batch2.write.format("delta").mode("append").save(delta_path)

        result = spark.read.format("delta").load(delta_path)
        assert result.count() == 4
