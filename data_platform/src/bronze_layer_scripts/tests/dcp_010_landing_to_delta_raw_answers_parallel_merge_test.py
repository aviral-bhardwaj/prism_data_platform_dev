import pytest
import json
import os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, IntegerType,
)

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None


# ---------------------------------------------------------------------------
# Inline helper functions from the Databricks notebook (not directly
# importable in a standard CI environment).
# ---------------------------------------------------------------------------

def create_mock_survey_file(dbutils, file_path, respondents_data):
    """Write respondent data as a JSON file via dbutils."""
    responses = []
    for resp_data in respondents_data:
        response = {"uuid": resp_data["uuid"], **{k: v for k, v in resp_data.items() if k != "uuid"}}
        responses.append(response)
    dbutils.fs.put(file_path, json.dumps(responses, indent=2), overwrite=True)
    return len(responses)


def get_json_files_test(dbutils, base_path, survey_id, layout_id):
    """Discover all JSON files for a given survey/layout under base_path."""
    dir_path = os.path.join(base_path, f"{survey_id}_{layout_id}")
    try:
        files = dbutils.fs.ls(dir_path)
    except Exception:
        return []
    return [
        {
            "path": fi.path,
            "name": fi.name,
            "size_bytes": fi.size,
            "modification_time": datetime.fromtimestamp(fi.modificationTime / 1000),
        }
        for fi in files
        if fi.name.endswith(".json")
    ]


def process_test_file_with_scd2(spark, dbutils, file_path, survey_id, layout_id, table_path):
    """Process a single JSON survey file with SCD Type 2 logic.

    Uses file-based Delta paths so tests are isolated and do not require a
    Databricks catalog.  Requires the ``delta`` package — callers should guard
    with ``pytest.importorskip("delta")`` before calling this function.
    """
    if DeltaTable is None:
        pytest.skip("delta-spark not installed")

    current_timestamp = datetime.now()
    schema = StructType([
        StructField("survey_id", StringType(), True),
        StructField("layout_id", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("id_survey_question", StringType(), True),
        StructField("value", StringType(), True),
        StructField("dte_create", TimestampType(), True),
        StructField("dte_update", TimestampType(), True),
        StructField("__START_AT", TimestampType(), True),
        StructField("__END_AT", TimestampType(), True),
    ])

    table_exists = os.path.isdir(table_path) and bool(os.listdir(table_path))

    file_content = dbutils.fs.head(file_path)
    responses = json.loads(file_content)

    rows = []
    for resp in responses:
        uuid = resp.get("uuid", "")
        for question_id, answer_value in resp.items():
            if question_id != "uuid":
                rows.append((
                    survey_id, layout_id, uuid, question_id, str(answer_value),
                    current_timestamp, current_timestamp, current_timestamp, None,
                ))

    new_df = spark.createDataFrame(rows, schema)
    respondent_count = len(responses)

    if not table_exists:
        new_df.write.format("delta").mode("overwrite").save(table_path)
        return respondent_count, len(rows)

    delta_table = DeltaTable.forPath(spark, table_path)

    active_before = (
        spark.read.format("delta").load(table_path)
        .filter(F.col("__END_AT").isNull())
        .select(
            "survey_id", "layout_id", "uuid", "id_survey_question",
            F.col("value").alias("existing_value"),
        )
    )

    delta_table.alias("target").merge(
        new_df.alias("source"),
        (
            "target.survey_id = source.survey_id AND "
            "target.layout_id = source.layout_id AND "
            "target.uuid = source.uuid AND "
            "target.id_survey_question = source.id_survey_question AND "
            "target.__END_AT IS NULL"
        ),
    ).whenMatchedUpdate(
        condition="target.value != source.value",
        set={"__END_AT": "source.__START_AT"},
    ).whenNotMatchedBySourceUpdate(
        set={"__END_AT": F.lit(current_timestamp)},
    ).execute()

    records_to_insert = (
        new_df.alias("new")
        .join(
            active_before.alias("existing"),
            on=["survey_id", "layout_id", "uuid", "id_survey_question"],
            how="left",
        )
        .filter(
            F.col("existing.existing_value").isNull()
            | (F.col("new.value") != F.col("existing.existing_value"))
        )
        .select(
            F.col("new.survey_id"), F.col("new.layout_id"), F.col("new.uuid"),
            F.col("new.id_survey_question"), F.col("new.value"),
            F.col("new.dte_create"), F.col("new.dte_update"),
            F.col("new.__START_AT"), F.col("new.__END_AT"),
        )
    )

    records_count = records_to_insert.count()
    if records_count > 0:
        records_to_insert.write.format("delta").mode("append").save(table_path)

    return respondent_count, records_count


def process_single_survey_test(spark, dbutils, base_path, survey_id, layout_id, table_path):
    """Process all JSON files for one survey/layout in chronological order."""
    files = get_json_files_test(dbutils, base_path, survey_id, layout_id)
    total_respondents = 0
    total_records = 0
    for file_info in sorted(files, key=lambda x: x["name"]):
        resp_count, rec_count = process_test_file_with_scd2(
            spark, dbutils, file_info["path"], survey_id, layout_id, table_path
        )
        total_respondents += resp_count
        total_records += rec_count
    return total_respondents, total_records


# ---------------------------------------------------------------------------
# Tests for create_mock_survey_file
# ---------------------------------------------------------------------------

class TestCreateMockSurveyFile:
    def test_creates_json(self, dbutils, tmp_path):
        file_path = str(tmp_path / "survey_test.json")
        respondents = [{"uuid": "uuid_001", "q1": "5", "q2": "Yes"}]
        create_mock_survey_file(dbutils, file_path, respondents)
        assert os.path.exists(file_path)
        with open(file_path) as fh:
            data = json.load(fh)
        assert isinstance(data, list)

    def test_respondent_count(self, dbutils, tmp_path):
        file_path = str(tmp_path / "survey_test.json")
        respondents = [
            {"uuid": "uuid_001", "q1": "5"},
            {"uuid": "uuid_002", "q1": "3"},
        ]
        count = create_mock_survey_file(dbutils, file_path, respondents)
        assert count == 2


# ---------------------------------------------------------------------------
# Tests for get_json_files_test
# ---------------------------------------------------------------------------

class TestGetJsonFiles:
    def test_finds_json_files(self, dbutils, tmp_path):
        survey_id, layout_id = "S001", "L001"
        survey_dir = tmp_path / f"{survey_id}_{layout_id}"
        survey_dir.mkdir()
        (survey_dir / "file1.json").write_text('[{"uuid": "u1"}]')
        (survey_dir / "file2.txt").write_text("not json")
        files = get_json_files_test(dbutils, str(tmp_path), survey_id, layout_id)
        assert len(files) == 1
        assert files[0]["name"] == "file1.json"

    def test_empty_for_missing_dir(self, dbutils, tmp_path):
        files = get_json_files_test(dbutils, str(tmp_path), "MISSING", "L999")
        assert files == []


# ---------------------------------------------------------------------------
# Tests for SCD Type 2 processing
# ---------------------------------------------------------------------------

class TestSCD2Processing:
    def test_initial_load(self, spark, dbutils, tmp_path):
        pytest.importorskip("delta")
        survey_id, layout_id = "S001", "L001"
        file_path = str(tmp_path / "data.json")
        table_path = str(tmp_path / "delta_table")

        respondents = [
            {"uuid": "u1", "q1": "5", "q2": "Yes"},
            {"uuid": "u2", "q1": "3", "q2": "No"},
        ]
        create_mock_survey_file(dbutils, file_path, respondents)

        resp_count, rec_count = process_test_file_with_scd2(
            spark, dbutils, file_path, survey_id, layout_id, table_path
        )

        assert resp_count == 2
        df = spark.read.format("delta").load(table_path)
        # 2 respondents × 2 questions = 4 rows, all active (__END_AT is null)
        assert df.count() == 4
        assert df.filter(F.col("__END_AT").isNull()).count() == 4

    def test_scd2_update(self, spark, dbutils, tmp_path):
        pytest.importorskip("delta")
        survey_id, layout_id = "S002", "L002"
        table_path = str(tmp_path / "delta_table")

        # Initial load
        file1 = str(tmp_path / "file1.json")
        create_mock_survey_file(dbutils, file1, [{"uuid": "u1", "q1": "5"}])
        process_test_file_with_scd2(spark, dbutils, file1, survey_id, layout_id, table_path)

        # Update — q1 changes from "5" to "4"
        file2 = str(tmp_path / "file2.json")
        create_mock_survey_file(dbutils, file2, [{"uuid": "u1", "q1": "4"}])
        process_test_file_with_scd2(spark, dbutils, file2, survey_id, layout_id, table_path)

        df = spark.read.format("delta").load(table_path)
        closed = df.filter(F.col("__END_AT").isNotNull())
        assert closed.count() >= 1

    def test_deleted_uuid_closed(self, spark, dbutils, tmp_path):
        pytest.importorskip("delta")
        survey_id, layout_id = "S003", "L003"
        table_path = str(tmp_path / "delta_table")

        # Initial load with uuid u1 and u2
        file1 = str(tmp_path / "file1.json")
        create_mock_survey_file(dbutils, file1, [
            {"uuid": "u1", "q1": "5"},
            {"uuid": "u2", "q1": "3"},
        ])
        process_test_file_with_scd2(spark, dbutils, file1, survey_id, layout_id, table_path)

        # Second snapshot — u2 is gone
        file2 = str(tmp_path / "file2.json")
        create_mock_survey_file(dbutils, file2, [{"uuid": "u1", "q1": "5"}])
        process_test_file_with_scd2(spark, dbutils, file2, survey_id, layout_id, table_path)

        df = spark.read.format("delta").load(table_path)
        u2_closed = df.filter((F.col("uuid") == "u2") & F.col("__END_AT").isNotNull())
        assert u2_closed.count() >= 1


# ---------------------------------------------------------------------------
# Tests for process_single_survey_test (end-to-end multi-file processing)
# ---------------------------------------------------------------------------

class TestProcessSingleSurvey:
    def test_processes_all_files(self, spark, dbutils, tmp_path):
        pytest.importorskip("delta")
        survey_id, layout_id = "S010", "L010"
        table_path = str(tmp_path / "delta_table")
        base_path = str(tmp_path)

        survey_dir = tmp_path / f"{survey_id}_{layout_id}"
        survey_dir.mkdir()

        # Create two chronological snapshots
        create_mock_survey_file(
            dbutils,
            str(survey_dir / "snap_001.json"),
            [{"uuid": "u1", "q1": "5"}, {"uuid": "u2", "q1": "3"}],
        )
        create_mock_survey_file(
            dbutils,
            str(survey_dir / "snap_002.json"),
            [{"uuid": "u1", "q1": "4"}, {"uuid": "u2", "q1": "3"}],
        )

        total_respondents, total_records = process_single_survey_test(
            spark, dbutils, base_path, survey_id, layout_id, table_path
        )

        # Two files processed → respondent counts summed across both files
        assert total_respondents == 4
        df = spark.read.format("delta").load(table_path)
        assert df.count() >= 2
