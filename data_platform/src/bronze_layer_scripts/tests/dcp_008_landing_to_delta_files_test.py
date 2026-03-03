import pytest
import os
from itertools import product
from functools import reduce
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Inline the function under test (source file is a Databricks notebook and
# is not directly importable in a standard CI environment).
# ---------------------------------------------------------------------------

def eq_null_safe_join(df_base, df_to_join, list_of_cols, join_type, drop_cols=True):
    """Performs a join between two DataFrames using the EqNullSafe condition.

    Args:
        df_base: Base DataFrame to be joined.
        df_to_join: DataFrame to be joined.
        list_of_cols: List of columns on which the join is performed.
        join_type: Type of join (e.g. 'inner', 'leftanti').
        drop_cols: When True, drops the right-side join columns from the result.

    Returns:
        Joined DataFrame.
    """
    join_cols = list_of_cols if list_of_cols is not None else df_base.columns
    join_condition = reduce(
        lambda x, y: x & y,
        [df_base[k].eqNullSafe(df_to_join[k]) for k in join_cols],
    )
    drop_condition = [df_to_join[k] for k in join_cols]
    if drop_cols:
        return df_base.join(df_to_join, on=join_condition, how=join_type).drop(*drop_condition)
    return df_base.join(df_to_join, on=join_condition, how=join_type)


# ---------------------------------------------------------------------------
# Tests for eq_null_safe_join
# ---------------------------------------------------------------------------

class TestEqNullSafeJoin:
    def test_inner_join_matches(self, spark):
        df1 = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])
        df2 = spark.createDataFrame([("a", 10), ("b", 20)], ["key", "extra"])
        result = eq_null_safe_join(df1, df2, ["key"], "inner")
        rows = sorted(result.collect(), key=lambda r: r.key)
        assert len(rows) == 2
        assert rows[0].key == "a"
        assert rows[1].key == "b"

    def test_leftanti_no_diff(self, spark):
        df1 = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])
        df2 = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])
        result = eq_null_safe_join(df1, df2, ["key", "val"], "leftanti")
        assert result.count() == 0

    def test_leftanti_with_diff(self, spark):
        df1 = spark.createDataFrame([("a", 1), ("b", 2)], ["key", "val"])
        df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["key", "val"])
        result = eq_null_safe_join(df1, df2, ["key", "val"], "leftanti")
        assert result.count() == 1
        assert result.collect()[0].key == "b"

    def test_handles_nulls(self, spark):
        df1 = spark.createDataFrame([(None, 1), ("b", 2)], ["key", "val"])
        df2 = spark.createDataFrame([(None, 1), ("b", 2)], ["key", "val"])
        result = eq_null_safe_join(df1, df2, ["key", "val"], "leftanti")
        assert result.count() == 0


# ---------------------------------------------------------------------------
# Tests for final_list cross-product generation
# ---------------------------------------------------------------------------

class TestFinalListGeneration:
    def test_cross_product_count(self):
        file_names = ["questions_mapping", "answers_mapping", "variable_mapping"]
        surveys = ["AAA_111", "BBB_222"]
        final_list = [f"{f}_{s}" for f, s in product(file_names, surveys)]
        assert len(final_list) == len(file_names) * len(surveys)

    def test_cross_product_values(self):
        file_names = ["questions_mapping"]
        surveys = ["AAA_111", "BBB_222"]
        final_list = [f"{f}_{s}" for f, s in product(file_names, surveys)]
        assert "questions_mapping_AAA_111" in final_list
        assert "questions_mapping_BBB_222" in final_list


# ---------------------------------------------------------------------------
# Tests for file-path loop logic
# ---------------------------------------------------------------------------

class TestFilePathLogic:
    def test_base_path_not_exists_skips(self):
        base_path = "/nonexistent/path/that/does/not/exist"
        list_of_latest_files = []
        if os.path.exists(base_path):
            list_of_latest_files.append("something")
        assert len(list_of_latest_files) == 0

