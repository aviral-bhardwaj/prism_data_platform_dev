# data_platform/src/bronze_layer_scripts/tests/conftest.py
import builtins
import pytest
from pyspark.sql import SparkSession

try:
    from delta import configure_spark_with_delta_pip

    _builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("prism-bronze-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    _spark = configure_spark_with_delta_pip(_builder).getOrCreate()
except ImportError:
    _spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("prism-bronze-tests")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

# Inject as builtin — resolves module-level `spark` during collection
builtins.spark = _spark


@pytest.fixture(scope="session")
def spark():
    return _spark
