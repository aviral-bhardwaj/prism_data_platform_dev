# data_platform/src/bronze_layer_scripts/tests/conftest.py
import builtins
import os
import shutil
import pytest
from collections import namedtuple
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


# ---------------------------------------------------------------------------
# dbutils stub — resolves module-level `dbutils` references during collection
# ---------------------------------------------------------------------------

_FileInfo = namedtuple("FileInfo", ["path", "name", "size", "modificationTime"])


class _FsStub:
    def mkdirs(self, path):
        os.makedirs(path, exist_ok=True)

    def put(self, path, content, overwrite=False):
        if not overwrite and os.path.exists(path):
            raise FileExistsError(path)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as fh:
            fh.write(content)

    def ls(self, path):
        entries = []
        for name in os.listdir(path):
            full = os.path.join(path, name)
            stat = os.stat(full)
            entries.append(_FileInfo(
                path=full,
                name=name,
                size=stat.st_size,
                modificationTime=int(stat.st_mtime * 1000),
            ))
        return entries

    def head(self, path, max_bytes=65536):
        with open(path, "r") as fh:
            return fh.read(max_bytes)

    def rm(self, path, recurse=False):
        if not os.path.exists(path):
            return
        if os.path.isdir(path):
            if recurse:
                shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)


class _DBUtils:
    def __init__(self):
        self.fs = _FsStub()


# Inject as builtin — resolves module-level `dbutils` during collection
builtins.dbutils = _DBUtils()


@pytest.fixture(scope="session")
def spark():
    return _spark


@pytest.fixture(scope="session")
def dbutils():
    return builtins.dbutils
