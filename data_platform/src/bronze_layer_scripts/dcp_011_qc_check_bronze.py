# Databricks notebook source
# MAGIC %pip install great_expectations==1.5.7
# MAGIC %pip install 'great_expectations[spark]'

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
import great_expectations as gx

# COMMAND ----------

# Read all necessary base tables
all_survey_df = spark.table('prism_bronze.decipher.all_surveys').where(F.col('__END_AT').isNull())
survey_layouts_df = spark.table('prism_bronze.decipher.survey_layouts').where(F.col('__END_AT').isNull())
survey_metadata_df = spark.table('prism_bronze.decipher.survey_metadata').where(F.col('__END_AT').isNull())


# Create list of survey results tables based on survey and layout ids
surveys_df = spark.table('prism_bronze.data_plat_control.surveys')
survey_layout_ids_list = [(row['survey_id'], row['layout_id']) for row in surveys_df.select('survey_id','layout_id').collect()]

surveys_raw_data_tuples = []
for id_pair in survey_layout_ids_list:
    survey_id = id_pair[0]
    layout_id = id_pair[1]
    try:
        survey_raw_data_df = spark.table(f'prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}').where(F.col('__END_AT').isNull())
    except:
        survey_raw_data_df = spark.table(f'prism_bronze.decipher_raw_answers.survey_responses_{survey_id}_{layout_id}').where(F.col('deleted_in_source')==False)
        
    surveys_raw_data_tuples.append((survey_id, layout_id, survey_raw_data_df))

# COMMAND ----------

def no_duplicate_check(table_name, df):
    test = gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list= [col for col in df.columns if col not in ['_rescued_data', 'bronze_updated_at', 'file_name', '__START_AT', '__END_AT']]
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def unique_column_check(table_name, df, col):
    test = gx.expectations.ExpectColumnValuesToBeUnique(
        column=col
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def non_null_column_check(table_name, df, col):
    test = gx.expectations.ExpectColumnValuesToNotBeNull(
        column=col
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def all_null_column_check(table_name, df, col):
    test = gx.expectations.ExpectColumnValuesToBeNull(
        column=col
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def str_len_column_check(table_name : str, df, col : str, lengh : int):
    test = gx.expectations.ExpectColumnValueLengthsToEqual(
        column=col,
        value=lengh
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def schema_check(table_name : str, df, col_set):
    test = gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=col_set,
        exact_match=False
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

def special_characters_check(table_name : str, df, col):
    UNWANTED_REGEX = r"[‘’ʼ`′＇“”″＂–—―−－\u200B\u00A0]" 
    test = gx.expectations.ExpectColumnValuesToNotMatchRegex(
        column=col,
        regex=UNWANTED_REGEX
    )

    context = gx.get_context()

    data_source = context.data_sources.add_spark(name=f'{table_name}_source')
    data_asset = data_source.add_dataframe_asset(name=f'{table_name}_asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f'{table_name}_batch'
    )

    batch_parameters = {"dataframe": df,
                        }

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    # Test the Expectation
    validation_results = batch.validate(test)
    return validation_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicate Rows Checks

# COMMAND ----------

tables_to_check_duplication = [
    ("all_surveys_table", all_survey_df),
    ("survey_layouts_table", survey_layouts_df),
    ("survey_metadata_table", survey_metadata_df),
    *[(f"raw_data_{i[0]}_{i[1]}", i[2]) for i in surveys_raw_data_tuples]
]
val_results = []

for t in tables_to_check_duplication:
    validation = no_duplicate_check(t[0], t[1])
    val_results.append((t[0], 'not_aplicable', validation))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Column have no null values Check

# COMMAND ----------

tables_to_check_no_null_values = [
    ("all_surveys_table", all_survey_df, 'path'),
    ("all_surveys_table", all_survey_df, 'state'),
    ("all_surveys_table", all_survey_df, 'tags'),
    ("survey_layouts_table", survey_layouts_df, 'id'),
    ("survey_metadata_table", survey_metadata_df, 'label'),
    *[(f"raw_data_{i[0]}_{i[1]}", i[2], 'uid_quality_starts') for i in surveys_raw_data_tuples if 'uid_quality_starts' in i[2].columns],
    # *[(f"raw_data_{i[0]}_{i[1]}", i[2].where(), 'uid_quality_starts') for i in surveys_raw_data_tuples],
]

uid_country_items = [
    (f"raw_data_{i[0]}_{i[1]}", i[2], 'uid_country')
    for i in surveys_raw_data_tuples if hasattr(i[2], "columns") and 'uid_country' in i[2].columns
]

if uid_country_items:
    tables_to_check_no_null_values.extend(uid_country_items)

for t in tables_to_check_no_null_values:
    validation = non_null_column_check(t[0], t[1], t[2])
    val_results.append((t[0], t[2], validation))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column have All null values Check

# COMMAND ----------

tables_to_check_all_null_values = [
    *[(f"raw_data_{i[0]}_{i[1]}", i[2].where(F.col('status')=='3'), 'CV_TermCode') for i in surveys_raw_data_tuples if 'CV_TermCode' in i[2].columns],
]

for t in tables_to_check_all_null_values:
    validation = all_null_column_check(t[0], t[1], t[2])
    val_results.append((t[0], t[2], validation))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Check

# COMMAND ----------

all_surveys_columns = ['accessed',
 'active',
 'archived',
 'averageQtime',
 'category',
 'clickthrough',
 'closedDate',
 'compat',
 'createdBy',
 'createdOn',
 'dateLaunched',
 'description',
 'directory',
 'favorite',
 'finishTime',
 'groups',
 'hasDashboard',
 'hasProjectParameters',
 'hasSavedReport',
 'isCATI',
 'isDeprecated',
 'isRetired',
 'lang',
 'lastAccess',
 'lastAccessBy',
 'lastEdit',
 'lastEditBy',
 'lastQuotaEdit',
 'lastSurveyEdit',
 'medianQtime',
 'myAccess',
 'myEdit',
 'otherLanguages',
 'owner',
 'path',
 'qualified',
 'questions',
 'retention',
 'sampleSources',
 'startTime',
 'state',
 'tags',
 'title',
 'today',
 'total',
 'type',
 '_rescued_data',
 'bronze_updated_at',
 'file_name',
 '__START_AT',
 '__END_AT']

surveys_layout_columns = ['createdBy',
 'createdOn',
 'description',
 'id',
 'updatedBy',
 'updatedOn',
 'variables',
 '_rescued_data',
 'bronze_updated_at',
 'file_name',
 'survey_id',
 '__START_AT',
 '__END_AT']

surveys_metadata_columns = ['col',
 'colTitle',
 'label',
 'qlabel',
 'qtitle',
 'rating',
 'row',
 'rowTitle',
 'title',
 'type',
 'value',
 'values',
 'vgroup',
 'bronze_updated_at',
 'file_name',
 'survey_id',
 'layout_id',
 '__START_AT',
 '__END_AT']

raw_data_columns = ['QSamp',
 'qtime',
 'start_date',
 'status',
 'uuid',
 '_rescued_data',
 'row_hash',
 'bronze_updated_at',
 'file_name',
 'survey_id',
 'layout_id',
 '__START_AT',
 '__END_AT']


tables_to_check_schema = [
    ("all_surveys_table", all_survey_df, all_surveys_columns),
    ("survey_layouts_table", survey_layouts_df, surveys_layout_columns),
    ("survey_metadata_table", survey_metadata_df, surveys_metadata_columns),
    *[(f"raw_data_{i[0]}_{i[1]}", i[2], raw_data_columns) for i in surveys_raw_data_tuples],
    # *[(f"raw_data_{i[0]}_{i[1]}", i[2].where(), 'uid_quality_starts') for i in surveys_raw_data_tuples],
]

for t in tables_to_check_schema:
    validation = schema_check(t[0], t[1], t[2])
    val_results.append((t[0], t[2], validation))

# COMMAND ----------

# MAGIC %md
# MAGIC # Verbatim Checks

# COMMAND ----------

# Create verbatim tuples
surveys_raw_data_verbatim_tuples = []
surveys_raw_data_open_ended_tuples = []

for t in surveys_raw_data_tuples:
    table_name = f"raw_data_{t[0]}_{t[1]}"
    df = t[2]
    verbatim_cols = [(table_name, df, col.name, 5000) for col in df.schema if 'nps_why' in col.name.lower()]
    open_ended_cols = [(table_name, df, col.name, 1024) for col in df.schema if 'oe' in col.name.lower() or 'other' in col.name.lower()]
    surveys_raw_data_verbatim_tuples.extend(verbatim_cols)
    surveys_raw_data_open_ended_tuples.extend(open_ended_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Special character check

# COMMAND ----------

# Verbatim columns
for t in surveys_raw_data_verbatim_tuples:
  validation = special_characters_check(t[0], t[1], t[2])
  val_results.append((t[0], t[2], validation))

#Open ended column
for t in surveys_raw_data_open_ended_tuples:
  validation = special_characters_check(t[0], t[1], t[2])
  val_results.append((t[0], t[2], validation))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Truncated Text check

# COMMAND ----------

trunc_val_results = []

# Verbatim columns
for t in surveys_raw_data_verbatim_tuples:
  validation = str_len_column_check(t[0], t[1], t[2], t[3])
  trunc_val_results.append((t[0], t[2], validation))

#Open ended column
for t in surveys_raw_data_open_ended_tuples:
  validation = str_len_column_check(t[0], t[1], t[2], t[3])
  trunc_val_results.append((t[0], t[2], validation))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create result tables

# COMMAND ----------

trunc_val_results

# COMMAND ----------

result_tuples = []

# Non Text Tuncation Tests
for result in val_results:
    table_name = result[0]
    col_name = result[1]
    test_name = result[2]["expectation_config"]["type"]
    test_status = result[2]["success"]
    result_dict = result[2]["result"]
    if "partial_unexpected_list" in result_dict:
        partial_unexpected_list = result_dict["partial_unexpected_list"]
    elif "partial_unexpected_list" not in result_dict and test_status == False:
        partial_unexpected_list = result_dict["details"]["mismatched"]["missing"]
    else:
        partial_unexpected_list = []
    text_trunc_perc = -1.0

    result_tuples.append((table_name, test_name, col_name, test_status, partial_unexpected_list, text_trunc_perc))

# Text Tuncation Tests
for result in trunc_val_results:
    table_name = result[0]
    col_name = result[1]
    test_name = result[2]["expectation_config"]["type"]
    test_status_raw = result[2]["success"]
    if test_status_raw:
        test_status = False
    else:
        test_status = True

    result_dict = result[2]["result"]
    element_count = result_dict["element_count"]
    missing_count = result_dict["missing_count"]
    not_def_len_count = result_dict["unexpected_count"]
    at_def_len_count = element_count - missing_count - not_def_len_count
    partial_unexpected_list = [f'Responses with length {result[2]["expectation_config"]["kwargs"]["value"]}: {at_def_len_count}']
    text_trunc_perc = at_def_len_count / (element_count - missing_count)

    result_tuples.append((table_name, test_name, col_name, test_status, partial_unexpected_list, text_trunc_perc))

# COMMAND ----------

spark.createDataFrame(result_tuples, schema="table_name string, test_name string, column_name string, test_status boolean, partial_unexpected_list array<string>, text_trunc_perc double").write.mode("overwrite").option("mergeSchema", "true").saveAsTable("prism_bronze.decipher.dcp_qc_table")