"""
Integration tests for the Gold layer star schema and analytics patterns.

Tests fact table joins, NPS calculation logic, and aggregation patterns
used in the US Hotels analytics domain.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class TestStarSchemaJoins:
    """Tests for star schema fact-dimension joins."""

    def test_fact_dimension_join(self, spark):
        """Fact table should correctly join to dimension tables on foreign keys."""
        dim_wave = spark.createDataFrame(
            [(1, 1, "101", "Phase 1", "25Q1"), (2, 1, "102", "Phase 1", "25Q2")],
            ["gid_wave", "gid_instrument", "survey_id", "survey_phase", "desc_wave"],
        )
        dim_question = spark.createDataFrame(
            [(10, "101", "201", "q1", "Satisfaction"), (20, "101", "201", "q2", "Recommend")],
            ["gid_question", "survey_id", "layout_id", "id_survey_question", "question_text"],
        )
        fact_response = spark.createDataFrame(
            [(1, 10, 1, 8), (2, 20, 1, 9), (3, 10, 2, 7)],
            ["gid_response", "gid_question", "gid_wave", "response_value"],
        )

        result = (
            fact_response
            .join(dim_question, on="gid_question", how="inner")
            .join(dim_wave, on="gid_wave", how="inner")
        )

        assert result.count() == 3
        assert "question_text" in result.columns
        assert "desc_wave" in result.columns

    def test_fact_response_filtering(self, spark):
        """Fact table should filter by gid_instrument correctly."""
        fact = spark.createDataFrame(
            [(1, 1, 8), (2, 1, 9), (3, 2, 7), (4, 2, 6)],
            ["gid_response", "gid_instrument", "response_value"],
        )
        filtered = fact.filter(F.col("gid_instrument") == 1)

        assert filtered.count() == 2


class TestNPSCalculation:
    """Tests for Net Promoter Score calculation patterns."""

    def test_nps_classification(self, spark):
        """NPS responses should be classified as Promoter (9-10), Passive (7-8), Detractor (0-6)."""
        data = [(i,) for i in range(11)]
        df = spark.createDataFrame(data, ["score"])

        classified = df.withColumn(
            "nps_category",
            F.when(F.col("score") >= 9, "Promoter")
            .when(F.col("score") >= 7, "Passive")
            .otherwise("Detractor"),
        )

        promoters = classified.filter(F.col("nps_category") == "Promoter").count()
        passives = classified.filter(F.col("nps_category") == "Passive").count()
        detractors = classified.filter(F.col("nps_category") == "Detractor").count()

        assert promoters == 2   # 9, 10
        assert passives == 2    # 7, 8
        assert detractors == 7  # 0-6

    def test_nps_score_calculation(self, spark):
        """NPS = %Promoters - %Detractors."""
        data = [
            ("Promoter",), ("Promoter",), ("Promoter",),
            ("Passive",), ("Passive",),
            ("Detractor",), ("Detractor",),
            ("Detractor",), ("Detractor",), ("Detractor",),
        ]
        df = spark.createDataFrame(data, ["nps_category"])
        total = df.count()

        promoter_pct = df.filter(F.col("nps_category") == "Promoter").count() / total * 100
        detractor_pct = df.filter(F.col("nps_category") == "Detractor").count() / total * 100
        nps = promoter_pct - detractor_pct

        # 30% promoters - 50% detractors = -20
        assert nps == -20.0

    def test_nps_by_wave(self, spark):
        """NPS should be calculable per wave/period."""
        data = [
            ("25Q1", 10), ("25Q1", 9), ("25Q1", 5), ("25Q1", 3),
            ("25Q2", 10), ("25Q2", 10), ("25Q2", 8), ("25Q2", 7),
        ]
        df = spark.createDataFrame(data, ["wave", "score"])
        df = df.withColumn(
            "nps_category",
            F.when(F.col("score") >= 9, "Promoter")
            .when(F.col("score") >= 7, "Passive")
            .otherwise("Detractor"),
        )

        nps_by_wave = (
            df.groupBy("wave")
            .agg(
                (F.sum(F.when(F.col("nps_category") == "Promoter", 1).otherwise(0)) / F.count("*") * 100
                 - F.sum(F.when(F.col("nps_category") == "Detractor", 1).otherwise(0)) / F.count("*") * 100
                ).alias("nps_score"),
            )
            .orderBy("wave")
            .collect()
        )

        # Q1: 2 promoters / 4 total (50%) - 2 detractors / 4 total (50%) = 0
        assert nps_by_wave[0]["nps_score"] == 0.0
        # Q2: 2 promoters / 4 total (50%) - 0 detractors / 4 total (0%) = 50
        assert nps_by_wave[1]["nps_score"] == 50.0


class TestAggregationPatterns:
    """Tests for aggregation patterns used in Gold layer analytics."""

    def test_group_by_aggregation(self, spark):
        """Group-by aggregation with multiple metrics."""
        data = [
            ("Hotel A", "25Q1", 8), ("Hotel A", "25Q1", 9),
            ("Hotel A", "25Q2", 7), ("Hotel B", "25Q1", 10),
            ("Hotel B", "25Q1", 6), ("Hotel B", "25Q2", 8),
        ]
        df = spark.createDataFrame(data, ["provider", "wave", "score"])

        agg_df = df.groupBy("provider", "wave").agg(
            F.avg("score").alias("avg_score"),
            F.count("*").alias("response_count"),
            F.min("score").alias("min_score"),
            F.max("score").alias("max_score"),
        )

        assert agg_df.count() == 4  # 2 providers x 2 waves
        hotel_a_q1 = agg_df.filter(
            (F.col("provider") == "Hotel A") & (F.col("wave") == "25Q1")
        ).collect()[0]
        assert hotel_a_q1["avg_score"] == 8.5
        assert hotel_a_q1["response_count"] == 2

    def test_window_function_ranking(self, spark):
        """Window functions for ranking within partitions."""
        from pyspark.sql.window import Window

        data = [
            ("25Q1", "Hotel A", 85), ("25Q1", "Hotel B", 92),
            ("25Q1", "Hotel C", 78), ("25Q2", "Hotel A", 90),
            ("25Q2", "Hotel B", 88),
        ]
        df = spark.createDataFrame(data, ["wave", "provider", "satisfaction_score"])

        window = Window.partitionBy("wave").orderBy(F.desc("satisfaction_score"))
        ranked = df.withColumn("rank", F.row_number().over(window))

        q1_top = ranked.filter((F.col("wave") == "25Q1") & (F.col("rank") == 1)).collect()[0]
        assert q1_top["provider"] == "Hotel B"

        q2_top = ranked.filter((F.col("wave") == "25Q2") & (F.col("rank") == 1)).collect()[0]
        assert q2_top["provider"] == "Hotel A"
