"""
Shared fixtures for integration tests.

Provides a local SparkSession with Delta Lake support for testing
PySpark transformations without a live Databricks cluster.
"""

import os
import shutil
import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession with Delta Lake for integration testing."""
    warehouse_dir = os.path.join(os.path.dirname(__file__), "_spark_warehouse")
    derby_dir = os.path.join(os.path.dirname(__file__), "_derby")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("prism_integration_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session

    session.stop()

    # Clean up temp directories
    for d in (warehouse_dir, derby_dir):
        if os.path.exists(d):
            shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_survey_data(spark):
    """Sample survey metadata matching Decipher API structure."""
    data = [
        ("US Hotels 25Q1", ["nam_instrument:US Hotels", "nam_country:US", "wave:25Q1", "instrument_detail:Phase 1"],
         "2025-01-15", "2025-03-15", "live", "selfserve/53a/250101", 101, 201),
        ("US Hotels 25Q2", ["nam_instrument:US Hotels", "nam_country:US", "wave:25Q2", "instrument_detail:Phase 1"],
         "2025-04-01", None, "live", "selfserve/53a/250102", 102, 202),
        ("Brazil NPS 2025", ["nam_instrument:Brazil NPS", "nam_country:BR", "wave:2025", "instrument_detail:Annual"],
         "2025-01-01", "2025-12-31", "closed", "selfserve/53a/250103", 103, 203),
    ]

    return spark.createDataFrame(
        data,
        ["title", "tags", "dateLaunched", "closedDate", "state", "path", "survey_id", "layout_id"],
    )


@pytest.fixture
def sample_respondent_data(spark):
    """Sample respondent data for fact table tests."""
    data = [
        (1, 101, 201, "Complete", "Yes", "2025-02-10 08:30:00"),
        (2, 101, 201, "Complete", "Yes", "2025-02-11 09:15:00"),
        (3, 101, 201, "Partial", "Yes", "2025-02-12 10:00:00"),
        (4, 102, 202, "Complete", "No", "2025-04-05 14:20:00"),
        (5, 102, 202, "Complete", "Yes", "2025-04-06 11:45:00"),
    ]
    return spark.createDataFrame(
        data,
        ["respondent_id", "survey_id", "layout_id", "qualified_status", "quality_start", "response_date"],
    )


@pytest.fixture
def sample_question_data(spark):
    """Sample question data for dimension tests."""
    data = [
        ("q1", 101, 201, "single", "Overall Satisfaction", 1),
        ("q2", 101, 201, "multiple", "Amenities Used", 0),
        ("q3", 101, 201, "multiple", "Amenities Used", 1),
        ("q4", 101, 201, "open", "Additional Comments", 0),
        ("q5", 102, 202, "single", "Likelihood to Recommend", 1),
    ]
    return spark.createDataFrame(
        data,
        ["id_survey_question", "survey_id", "layout_id", "typ_question", "question_text", "num_answer"],
    )
