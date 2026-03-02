"""
Pytest configuration file for Databricks testing environment.
Provides mocks for Databricks globals (spark, dbutils) that are available
in Databricks notebooks but not in standard Python environments.
"""

import os
import pytest
from unittest.mock import MagicMock, Mock
import sys


@pytest.fixture(scope="session", autouse=True)
def setup_databricks_environment(request):
    """
    Setup Databricks environment by providing mock objects for:
    - spark: SparkSession mock
    - dbutils: Databricks utilities mock
    """
    # Skip mock injection for integration tests (they use a real SparkSession)
    if any("integration" in os.path.normpath(str(arg)).split(os.sep) for arg in request.config.args):
        yield
        return

    # Create spark session mock
    spark_mock = MagicMock()
    spark_mock.sql = MagicMock(return_value=MagicMock())
    spark_mock.read = MagicMock()
    spark_mock.read.parquet = MagicMock(return_value=MagicMock())
    spark_mock.read.csv = MagicMock(return_value=MagicMock())
    spark_mock.read.format = MagicMock(return_value=MagicMock())
    spark_mock.write = MagicMock()
    spark_mock.write.mode = MagicMock(return_value=MagicMock())
    spark_mock.write.format = MagicMock(return_value=MagicMock())
    spark_mock.conf = MagicMock()
    spark_mock.conf.set = MagicMock(return_value=None)
    spark_mock.conf.get = MagicMock(return_value="false")
    spark_mock.udf = MagicMock()
    spark_mock.udf.register = MagicMock(return_value=None)
    
    # Create dbutils mock
    dbutils_mock = MagicMock()
    dbutils_mock.widgets = MagicMock()
    dbutils_mock.widgets.get = MagicMock(return_value="test_value")
    dbutils_mock.widgets.getAll = MagicMock(return_value={})
    dbutils_mock.fs = MagicMock()
    dbutils_mock.fs.ls = MagicMock(return_value=[])
    dbutils_mock.fs.mkdirs = MagicMock(return_value=None)
    dbutils_mock.fs.rm = MagicMock(return_value=None)
    dbutils_mock.fs.cp = MagicMock(return_value=None)
    dbutils_mock.notebook = MagicMock()
    dbutils_mock.notebook.run = MagicMock(return_value="")
    dbutils_mock.jobs = MagicMock()
    dbutils_mock.jobs.taskValues = MagicMock()
    dbutils_mock.secrets = MagicMock()
    dbutils_mock.secrets.get = MagicMock(return_value="secret_value")
    
    # Add to builtins so they're available everywhere
    import builtins
    builtins.spark = spark_mock
    builtins.dbutils = dbutils_mock
    
    yield
    
    # Cleanup (optional)
    if hasattr(builtins, 'spark'):
        delattr(builtins, 'spark')
    if hasattr(builtins, 'dbutils'):
        delattr(builtins, 'dbutils')


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset all mocks before each test."""
    import builtins
    if hasattr(builtins, 'spark'):
        builtins.spark.reset_mock()
    if hasattr(builtins, 'dbutils'):
        builtins.dbutils.reset_mock()
    yield


def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )