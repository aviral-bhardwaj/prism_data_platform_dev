# Databricks notebook source
# Imports
from pyspark.sql import functions as F
from pyspark.sql import types as T

from datetime import *
import json

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# Setting up variables
bronze_layer = 'prism_bronze'
silver_layer = 'prism_silver'

raw_answers_catalog = 'decipher_raw_answers'
canonical_tables_catalog = 'canonical_tables'

dim_wave_table = "dim_wave"
fact_response_table = 'fact_response'
long_format_table = 'stg_survey_responses_long_format'
dim_respondent_table = 'dim_respondent'
dim_variable_mapping_table = 'dim_variable_mapping'
dim_question_table = 'dim_question'
fact_response_table = 'fact_response'

# COMMAND ----------

# Defining schema for the checks table
checks_table_schema = T.StructType([
        T.StructField("table_name", T.StringType(), True),
        T.StructField("column_name", T.StringType(), True),
        T.StructField("test_name", T.StringType(), True),
        T.StructField("test_status", T.StringType(), True),
        T.StructField("partial_unexpected_list", T.ArrayType(T.StringType()), True),
        T.StructField("comments", T.StringType(), True),
        T.StructField("dte_create", T.TimestampType(), True),
    ])

# COMMAND ----------

# Checks Respondents

# All respondents in bronze are present in silver?
# Getting raw data
list_of_available_surveys_numbers = [item[4] for item in spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_wave_table}").where(F.col('active')==True).collect()]

list_of_available_surveys = [
    f"{bronze_layer}.{raw_answers_catalog}.survey_responses_{item[-1]}" for item in (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{dim_wave_table}").where(F.col('active')==True)
    .select('survey_id', 'layout_id')
    .withColumn('file_name', F.concat(F.col('survey_id'), F.lit("_"), F.col('layout_id'))))
    .collect()
    ]

# Added filter for existing data
df_survey_metadata = [f'{bronze_layer}.{raw_answers_catalog}.{responses_file_name[0]}' for responses_file_name in spark.sql("select distinct concat('survey_responses_', survey_id, '_', layout_id) filename from prism_bronze.decipher.survey_metadata").collect()]

list_of_available_surveys = list(set(list_of_available_surveys) & set(df_survey_metadata))

# COMMAND ----------

if len(list_of_available_surveys) == 0:

    dbutils.notebook.exit("No data to process.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking count of distinct respondents along the tables

# COMMAND ----------

df_count_bronze_total = None

for table_name in list_of_available_surveys:

    # Count Respondents Bronze Layer
    df_bronze = (
        spark
        .table(table_name)
        .groupBy('survey_id', 'layout_id')
        .count()
        .withColumnRenamed('count', 'count_bronze')
        .withColumn('count_bronze', F.col('count_bronze').cast('integer'))
        )
    
    # Stack DataFrames
    if df_count_bronze_total is None:
        df_count_bronze_total = df_bronze
    else:
        df_count_bronze_total = df_count_bronze_total.unionByName(df_bronze)

# Selecting wave
dim_wave_select = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{dim_wave_table}")
    .select('gid_instrument', 'gid_wave', 'survey_id', 'layout_id')
    )

# Count Respondents Fact Response
df_fact_response = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{fact_response_table}")
    .where(F.col('deleted_in_source')==False)
    .select('gid_wave', 'gid_instrument', 'gid_respondent')
    .distinct()
    .groupBy('gid_wave', 'gid_instrument')
    .count()
    .withColumnRenamed('count', 'count_fact_response')
    .join(
        dim_wave_select,
        on=['gid_wave', 'gid_instrument'],
        how='left'
        )
    .drop('gid_instrument', 'gid_wave')
    )

# Count Respondents Long Format
df_long_format = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{long_format_table}")
    .where(F.col('deleted_in_source')==False)
    .select('survey_id', 'layout_id', 'uuid')
    .distinct()
    .groupBy('survey_id', 'layout_id')
    .count()
    .withColumnRenamed('count', 'count_long_format')
    )


# Count Respondents dim_respondent
df_respondent = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{dim_respondent_table}")
    .groupBy('gid_wave', 'gid_instrument')
    .count()
    .withColumnRenamed('count', 'count_dim_respondent')
    .join(
        dim_wave_select,
        on=['gid_wave', 'gid_instrument'],
        how='left'
        )
    .drop('gid_instrument', 'gid_wave')
    )


# Final table - Joining all dataframes
df_final_respondent_counts = (
    df_count_bronze_total
    .join(df_long_format, on=['survey_id', 'layout_id'], how='left')
    .join(df_fact_response, on=['survey_id', 'layout_id'], how='left')
    .join(df_respondent, on=['survey_id', 'layout_id'], how='left')
    )

conditions = (
    df_final_respondent_counts
    .where(
        (F.col('count_bronze')!=F.col('count_long_format')) 
        & ((F.col('count_fact_response')!=F.col('count_long_format')) 
        & (F.col('count_fact_response')!=F.col('count_dim_respondent')))
        )
    )
                                              
current_timestamp = datetime.now()

if conditions.count() == 0:
    new_record = [(
        'multiple',
        f'not applicable',
        'Respondents count check',
        'PASSED',
        None,
        'All respondents are present in all relevant tables (bronze_layer, stg_long, dim_respondent and fact_response).',
        current_timestamp
    )]

else:
    qc_array = [json.dumps(row.asDict()) for row in conditions.collect()]
    
    new_record = [(
        'multiple',
        f'not applicable',
        'Respondents count check',
        'FAILED',
        qc_array,
        'There is a mismatch of respondents among the tables.',
        current_timestamp
    )]

# Create DataFrame and append to table
new_df = spark.createDataFrame(new_record, checks_table_schema)
new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking respondents per demographic category

# COMMAND ----------

# Getting id_survey_questions for relevant universal ids
df_dim_question = spark.sql(f"""
                            select 
                                gid_question, id_survey_question, id_universal_question
                            from 
                                {silver_layer}.{canonical_tables_catalog}.{dim_question_table}
                            where
                                id_universal_question in ('uid_Age', 'uid_Gender', 'uid_Region', 'uid_HH_Income')
                            """)


list_of_id_survey_questions = list(set([item[1] for item in df_dim_question.collect()]))


df_survey_metadata = spark.sql(f"""
                                select 
                                    survey_id, layout_id, label, values 
                                from 
                                    prism_bronze.decipher.survey_metadata 
                                where 
                                    label in ('{"','".join(list_of_id_survey_questions + ['uuid', 'survey_id', 'layout_id'])}')
                                    and survey_id in ('{"','".join(list_of_available_surveys_numbers)}')
                                """)

#Intermediary step to be removed later, when we have one survey metadata table per survey_layout
df_survey_metadata = df_survey_metadata.drop('layout_id').distinct()

# Exploding columns
df_survey_metadata = (
    df_survey_metadata
    .withColumn('answers', F.explode('values'))
    .withColumn('answer', F.col('answers.value'))
    .withColumn('answer_title', F.col('answers.title'))
    .drop('answers', 'values')
    .distinct()
    .withColumnsRenamed({'label': 'id_survey_question', })
    )


df_bronze_demo_total = None

for table_name in list_of_available_surveys:

    list_of_cols = spark.table(table_name).columns  

    if 'key' in spark.table(table_name).columns:

        df_bronze_demo = (
            spark
            .table(table_name)
            .where(
                (F.col('deleted_in_source')==False)
                & (F.col('key').isin(list_of_id_survey_questions))
                )
            .select(['uuid', 'survey_id', 'layout_id', 'key', 'value'])
            .withColumnsRenamed(
                {
                    "key": "id_survey_question",
                    "value": "answer"
                }
            )
        )

    else:

        list_of_id_survey_questions_select = list(set(list_of_id_survey_questions) & set(list_of_cols))
               
        # Count Respondents Bronze Layer

        df_bronze_demo = (
            spark
            .table(table_name)
            .select(list_of_id_survey_questions_select + ['uuid', 'survey_id', 'layout_id'])
            .unpivot(['uuid', 'survey_id', 'layout_id'], list_of_id_survey_questions_select, "id_survey_question", "answer")
        )
    
    # Stack unpivoted DataFrames
    if df_bronze_demo_total is None:
        df_bronze_demo_total = df_bronze_demo
    else:
        df_bronze_demo_total = df_bronze_demo_total.unionByName(df_bronze_demo)

df_bronze_demo_total_final = (
    df_bronze_demo_total
    .join(df_survey_metadata, on=['survey_id', 'id_survey_question', 'answer'], how='left')
    .groupby('survey_id', 'id_survey_question', 'answer', 'answer_title')
    .count()
)

df_survey_metadata_final_count = (
    df_survey_metadata.alias("meta")
    .join(df_bronze_demo_total_final.alias("demo"), 
         on=(
             (F.col("demo.survey_id") == F.col("meta.survey_id"))
             & (F.col("demo.id_survey_question") == F.col("meta.id_survey_question"))
             & (F.col("demo.answer_title").eqNullSafe(F.col("meta.answer_title")))
         ),
         how='left')
    .select(
        F.col("meta.survey_id"), 
        F.col("meta.id_survey_question"), 
        F.col("meta.answer_title"), 
        F.col("demo.count").alias("count") 
    )
)

# Long format check
df_long_format = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{long_format_table}")
    .where(F.col('id_survey_question').isin(list_of_id_survey_questions))
    .groupby('survey_id', 'id_survey_question', 'txt_answer')
    .count()
    .withColumnsRenamed({'count': 'count_long_format', 'txt_answer': 'answer_title'})
    )

# fact_response check
df_fact_response = (
  spark.sql(f"""
            select
              dw.survey_id,
              dq.id_survey_question,
              fr.txt_answer answer_title,
              count(fr.gid_respondent) as count_fact_response
            from
              {silver_layer}.{canonical_tables_catalog}.{fact_response_table} fr
            left join 
              {silver_layer}.{canonical_tables_catalog}.{dim_respondent_table} dr
            on
              dr.gid_respondent = fr.gid_respondent
            left join
              {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} dw
            on dw.gid_wave = fr.gid_wave
            left join
              {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
            on dq.gid_question = fr.gid_question
            where
                dq.id_survey_question in ('{"','".join(list_of_id_survey_questions)}')
            group by all
            """)
        )


df_survey_metadata_final_count_join = (
    df_survey_metadata_final_count
    .join(df_long_format, on=['survey_id', 'id_survey_question', 'answer_title'], how='left')
    .join(df_fact_response, on=['survey_id', 'id_survey_question', 'answer_title'], how='left')
    )

conditions = (
    df_survey_metadata_final_count
    .where(F.col('count').isNull())
    )
                                              
current_timestamp = datetime.now()

if conditions.count() == 0:
    new_record = [(
        'multiple',
        f'not applicable',
        'Demographics check',
        'PASSED',
        None,
        'Every Demographics alternative in survey metadata has at least one answer associated with it.',
        current_timestamp
    )]

else:
    qc_array = [json.dumps(row.asDict()) for row in conditions.collect()]
    
    new_record = [(
        'multiple',
        f'not applicable',
        'Demographics check',
        'FAILED',
        qc_array,
        'There is at least one occurrence in the survey metadata that is not present in the bronze layer.',
        current_timestamp
    )]

# Create DataFrame and append to table
new_df = spark.createDataFrame(new_record, checks_table_schema)
new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking responses for every age bucket

# COMMAND ----------

# Getting generic 'Age' id_survey_question
age_question_ids = list(set([item[0] for item in spark.sql(f"""
                            select 
                                id_survey_question, id_universal_question
                            from 
                                {silver_layer}.{canonical_tables_catalog}.{dim_question_table}
                            where
                                id_universal_question in ('uid_Age')
                            """).collect()]))

age_data_bronze = (
    df_bronze_demo_total_final
    .where((F.col('id_survey_question').isin(age_question_ids)))
    .withColumn("age", F.col("answer").cast("int"))
    .withColumn("bucket_start", (F.floor(F.col("age") / 5) * 5).cast("int"))
    .withColumn("bucket_end", F.col('bucket_start') + 4)
    .withColumn("bucket", F.concat(F.col('bucket_start'), F.lit("-"), F.col("bucket_end")))
    .select("survey_id", "age", "answer", "bucket_start", "bucket_end", "bucket")
    .distinct()
    )

# age_data.display()

age_range = age_data_bronze.agg(
    F.min("age").alias("min_age"),
    F.max("age").alias("max_age")
).collect()[0]

# Calculate bucket boundaries
min_bucket = (age_range['min_age'] // 5) * 5
max_bucket = (age_range['max_age'] // 5) * 5

# Create expected buckets based on actual min/max
expected_buckets = [item[0] for item in (
    spark.range(min_bucket, max_bucket + 1, 5)
    .withColumnRenamed("id", "bucket_start")
    .withColumn("bucket_end", F.col("bucket_start") + 4)
    .withColumn("bucket", F.concat(F.col("bucket_start"), F.lit("-"), F.col("bucket_end")))
    .select("bucket")
    .collect()
    )]

existing_buckets = (
    age_data_bronze
    .select('survey_id', 'bucket')
    .distinct()
    .where(F.col('bucket').isNotNull())
    .toPandas()
    .groupby('survey_id')['bucket']
    .apply(list)
    .to_dict()
    )

conditions_bronze = []

for key, value in existing_buckets.items():

    comparison = list(set(existing_buckets[key]) ^ set(expected_buckets))

    if len(comparison)==0:
        continue
    conditions_bronze.append({'bronze_table':{key: comparison}})



# Long table
age_data_long_table = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{long_format_table}")
    .where((F.col('id_survey_question')=='Q_Age'))
    .withColumn("age", F.col("num_answer").cast("int"))
    .withColumn("bucket_start", (F.floor(F.col("age") / 5) * 5).cast("int"))
    .withColumn("bucket_end", F.col('bucket_start') + 4)
    .withColumn("bucket", F.concat(F.col('bucket_start'), F.lit("-"), F.col("bucket_end")))
    .select("survey_id", "age", "num_answer", "bucket_start", "bucket_end", "bucket")
    .distinct()    
    )

existing_buckets_long_table = (
    age_data_long_table
    .select('survey_id', 'bucket')
    .distinct()
    .where(F.col('bucket').isNotNull())
    .toPandas()
    .groupby('survey_id')['bucket']
    .apply(list)
    .to_dict()
    )

conditions_long_table = []

for key, value in existing_buckets_long_table.items():

    comparison = list(set(existing_buckets_long_table[key]) ^ set(expected_buckets))

    if len(comparison)==0:
        continue
    conditions_long_table.append({'stg_long_table':{key: comparison}})

# Fact Response

list_of_gid_question = [item[0] for item in df_dim_question.where(F.col('id_survey_question').isin(age_question_ids)).select('gid_question').collect()]

age_data_fact_response = (
    spark
    .table(f"{silver_layer}.{canonical_tables_catalog}.{fact_response_table}")
    .where((F.col('gid_question').isin(list_of_gid_question)))
    .withColumn("age", F.col("num_answer").cast("int"))
    .withColumn("bucket_start", (F.floor(F.col("age") / 5) * 5).cast("int"))
    .withColumn("bucket_end", F.col('bucket_start') + 4)
    .withColumn("bucket", F.concat(F.col('bucket_start'), F.lit("-"), F.col("bucket_end")))
    .select("gid_wave", "age", "num_answer", "bucket_start", "bucket_end", "bucket")
    .distinct()    
    )

existing_buckets_fact_response = (
    age_data_fact_response
    .select('gid_wave', 'bucket')
    .distinct()
    .where(F.col('bucket').isNotNull())
    .toPandas()
    .groupby('gid_wave')['bucket']
    .apply(list)
    .to_dict()
    )

conditions_fact_response = []

for key, value in existing_buckets_fact_response.items():

    comparison = list(set(existing_buckets_fact_response[key]) ^ set(expected_buckets))

    if len(comparison)==0:
        continue
    conditions_fact_response.append({'fact_response':{key: comparison}})

# Joining all conditions
final_age_bucket_conditions = conditions_bronze + conditions_long_table + conditions_fact_response

current_timestamp = datetime.now()

if len(final_age_bucket_conditions) == 0:
    new_record = [(
        'multiple',
        f'not applicable',
        'Age buckets check',
        'PASSED',
        None,
        'There is age data for every age bucket.',
        current_timestamp
        )]

else:   
    new_record = [(
        'multiple',
        f'not applicable',
        'Age buckets check',
        'FAILED',
        final_age_bucket_conditions,
        'There is data missing for one or more specific age bucket(s).',
        current_timestamp
        )]
    
# Create DataFrame and append to table
new_df = spark.createDataFrame(new_record, checks_table_schema)
new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking number of responses per respondent along the tables

# COMMAND ----------

df = None
for table_name in list_of_available_surveys:

    # Count Respondents Bronze Layer
    if 'key' in spark.table(table_name).columns:

        df_bronze_unpivoted = (
            spark
            .table(table_name)
            .select(['uuid', 'survey_id', 'layout_id', 'key', 'value'])
            .withColumnsRenamed(
                {
                    "key": "id_survey_question",
                    "value": "answer"
                }
            )
            .filter(F.col('answer').isNotNull())            
        ) 

    else:
        df_bronze = (
            spark
            .table(table_name)
            .where(F.col('__END_AT').isNull())
        )
        
        # Define columns to melt for this table
        cols_to_melt = [item for item in df_bronze.columns if item not in ['uuid', 'survey_id', 'layout_id', '__END_AT', '__START_AT', 'bronze_updated_at', '_rescued_data', 'row_hash', 'file_name']]
        cols_id = ['uuid', 'survey_id', 'layout_id']
        
        # Unpivot within the loop
        df_bronze_unpivoted = (
            df_bronze
            .unpivot(cols_id, cols_to_melt, "id_survey_question", "answer")
            .filter(F.col('answer').isNotNull())
            .drop( '__END_AT', '__START_AT', 'bronze_updated_at', '_rescued_data', 'row_hash', 'file_name')
        )
    
    # Stack unpivoted DataFrames
    if df is None:
        df = df_bronze_unpivoted
    else:
        df = df.unionByName(df_bronze_unpivoted, allowMissingColumns=True)

df_dim_question = spark.sql(f"""
  select 
    dq.id_survey_question,
    dw.survey_id,
    dq.typ_question
  from 
    {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
  left join 
    {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} dw
  on 
    dw.gid_instrument = dq.gid_instrument
  """)

df_responses_per_respondent = (
  df
  .join(df_dim_question, on=['id_survey_question', 'survey_id'], how='left')
  .filter(~((F.col('typ_question')=='multiple') & (F.col('answer')=='0')) & (F.col('id_survey_question')!='qsamp_1'))
  .groupby('uuid', 'survey_id', 'layout_id')
  .count()
  .withColumnRenamed('count', 'count_bronze')
)

# COMMAND ----------

df = None
for table_name in list_of_available_surveys:

    # Count Respondents Bronze Layer
    if 'key' in spark.table(table_name).columns:

        df_bronze_unpivoted = (
            spark
            .table(table_name)
            .select(['uuid', 'survey_id', 'layout_id', 'key', 'value'])
            .withColumnsRenamed(
                {
                    "key": "id_survey_question",
                    "value": "answer"
                }
            )
            .filter(F.col('answer').isNotNull())            
        ) 

    else:
        df_bronze = (
            spark
            .table(table_name)
            .where(F.col('__END_AT').isNull())
        )
        
        # Define columns to melt for this table
        cols_to_melt = [item for item in df_bronze.columns if item not in ['uuid', 'survey_id', 'layout_id', '__END_AT', '__START_AT', 'bronze_updated_at', '_rescued_data', 'row_hash', 'file_name']]
        cols_id = ['uuid', 'survey_id', 'layout_id']
        
        # Unpivot within the loop
        df_bronze_unpivoted = (
            df_bronze
            .unpivot(cols_id, cols_to_melt, "id_survey_question", "answer")
            .filter(F.col('answer').isNotNull())
            .drop( '__END_AT', '__START_AT', 'bronze_updated_at', '_rescued_data', 'row_hash', 'file_name')
        )
    
    # Stack unpivoted DataFrames
    if df is None:
        df = df_bronze_unpivoted
    else:
        df = df.unionByName(df_bronze_unpivoted, allowMissingColumns=True)

df_dim_question = spark.sql(f"""
  select 
    dq.id_survey_question,
    dw.survey_id,
    dq.typ_question
  from 
    {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
  left join 
    {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} dw
  on 
    dw.gid_instrument = dq.gid_instrument
  """)

df_responses_per_respondent = (
  df
  .join(df_dim_question, on=['id_survey_question', 'survey_id'], how='left')
  .filter(~((F.col('typ_question')=='multiple') & (F.col('answer')=='0')) & (F.col('id_survey_question')!='qsamp_1'))
  .groupby('uuid', 'survey_id', 'layout_id')
  .count()
  .withColumnRenamed('count', 'count_bronze')
)

# Count of number of responses per respondent on long table
df_long_table = (
  spark
  .table(f"{silver_layer}.{canonical_tables_catalog}.{long_format_table}")
  .select('uuid', 'survey_id', 'layout_id', 'id_survey_question')
  .filter((F.col('id_survey_question')!='qsamp_1') & (F.col('id_survey_question')!='qsamp') )
  .groupby('uuid', 'survey_id', 'layout_id')
  .count()
  .withColumnRenamed('count', 'count_long_format')
  )

# Count for fact_response
df_fact_response = (
  spark.sql(f"""
            select
              dr.id_respondent uuid,
              dw.survey_id,
              dw.layout_id,
              count(fr.gid_respondent) as count_fact_response
            from
              {silver_layer}.{canonical_tables_catalog}.{fact_response_table} fr
            left join 
              {silver_layer}.{canonical_tables_catalog}.{dim_respondent_table} dr
            on
              dr.gid_respondent = fr.gid_respondent
            left join
              {silver_layer}.{canonical_tables_catalog}.{dim_wave_table} dw
            on dw.gid_wave = fr.gid_wave
            left join
              {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
            on dq.gid_question = fr.gid_question
            where fr.deleted_in_source = false
            and dq.id_survey_question != 'qsamp'
            group by all
            """)

)


df_final = (
  df_responses_per_respondent
  .join(df_long_table, on=['uuid', 'survey_id', 'layout_id'], how='left')
  .join(df_fact_response, on=['uuid', 'survey_id', 'layout_id'], how='left')
  )

conditions = df_final.where((F.col('count_bronze')!=F.col('count_long_format')) & ((F.col('count_fact_response')!=F.col('count_long_format'))))
                                              
current_timestamp = datetime.now()

if conditions.count() == 0:
    new_record = [(
        'multiple',
        f'not applicable',
        'Responses per respondent check',
        'PASSED',
        None,
        'All responses per individual respondent are present in all relevant tables (bronze_layer, stg_long and fact_response)',
        current_timestamp
    )]

else:
    qc_array = [json.dumps(row.asDict()) for row in conditions.collect()]
    
    new_record = [(
        'multiple',
        f'not applicable',
        'Responses per respondent check',
        'FAILED',
        qc_array,
        'There is a mismatch of responses for certain/all respondents among the tables.',
        current_timestamp
    )]

# Create DataFrame and append to table
new_df = spark.createDataFrame(new_record, checks_table_schema)
new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking if variable mapping is correctly mapped in fact response

# COMMAND ----------

# QCs after mapping is done and original column_names are not dropped yet
list_of_relevant_cols = [
    'provider',
    'product',
    'episode',
    'channel',
    'initial_channel',
    'final_channel',
    ]

for column_name in list_of_relevant_cols[:]:

    df_var_map_to_check = spark.sql(f"""
    select distinct 
        fr.gid_instrument,
        fr.gid_wave,
        fr.gid_question,
        dq.id_survey_question,
        dq.id_universal_question,
        dvm.id_question,
        dvm.{column_name},
        fr.gid_{column_name}
    from {silver_layer}.{canonical_tables_catalog}.{fact_response_table} fr
    left join {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
    on dq.gid_question = fr.gid_question and dq.gid_instrument = fr.gid_instrument
    left join {silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table} dvm
    on dq.id_survey_question = dvm.id_question and fr.gid_instrument = dvm.gid_instrument
    where 1=1
        and fr.deleted_in_source = false 
        and ((fr.gid_instrument = 1 and fr.gid_wave = 1) or (fr.gid_instrument = 2))
        and {column_name} is not null
        and gid_{column_name} = -1
    order by dq.id_survey_question
    """)

    current_timestamp = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')

    if df_var_map_to_check.count() == 0:
        new_record = [(
            'fact_response',
            f'gid_{column_name}',
            'GID mappings check',
            'PASSED',
            None,
            'All GIDs were correctly mapped or mapping not applicable',
            current_timestamp
        )]
    else:
        qc_array = [json.dumps(row.asDict()) for row in df_var_map_to_check.collect()]
        
        new_record = [(
            'fact_response',
            f'gid_{column_name}',
            'GID mappings check',
            'FAILED',
            qc_array,
            'Partial GID mapping fail',
            current_timestamp
        )]

    # Create DataFrame and append to table
    new_df = spark.createDataFrame(new_record, checks_table_schema)
    new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking if key questions are answered (as per Prod Confluence Documentation)

# COMMAND ----------

# QCs after mapping is done and original column_names are not dropped yet
list_of_relevant_cols = [
    'provider',
    'product',
    'episode',
    'channel',
    'initial_channel',
    'final_channel',
    ]

for column_name in list_of_relevant_cols[:]:

    df_var_map_to_check = spark.sql(f"""
    select distinct 
        fr.gid_instrument,
        fr.gid_wave,
        fr.gid_question,
        dq.id_survey_question,
        dq.id_universal_question,
        dvm.id_question,
        dvm.{column_name},
        fr.gid_{column_name}
    from {silver_layer}.{canonical_tables_catalog}.{fact_response_table} fr
    left join {silver_layer}.{canonical_tables_catalog}.{dim_question_table} dq
    on dq.gid_question = fr.gid_question and dq.gid_instrument = fr.gid_instrument
    left join {silver_layer}.{canonical_tables_catalog}.{dim_variable_mapping_table} dvm
    on dq.id_survey_question = dvm.id_question and fr.gid_instrument = dvm.gid_instrument
    where 1=1
        and fr.deleted_in_source = false 
        and ((fr.gid_instrument = 1 and fr.gid_wave = 1) or (fr.gid_instrument = 2))
        and {column_name} is not null
        and gid_{column_name} = -1
        and dq.id_universal_question in (
            'uid_Provider_NPS',
            'uid_Provider_NPS_Verbatim',
            'uid_KPC_rating',
            'uid_Product_NPS',
            'uid_Product_NPS_Verbatim',
            'uid_Product_attributes',
            'uid_Episode_Screener',
            'uid_Episode_NPS',
            'uid_Episode_NPS_Verbatim',
            'uid_Channel_NPS',
            'uid_Channel_NPS_Verbatim',
            'uid_Channel_attributes'
        )
    order by dq.id_survey_question
    """)

    current_timestamp = datetime.now()#.strftime('%Y-%m-%d %H:%M:%S')

    if df_var_map_to_check.count() == 0:
        new_record = [(
            'fact_response',
            f'gid_{column_name}',
            'GID mappings check - key questions',
            'PASSED',
            None,
            'All GIDs were correctly mapped for key questions or mapping not applicable',
            current_timestamp
        )]
    else:
        qc_array = [json.dumps(row.asDict()) for row in df_var_map_to_check.collect()]
        
        new_record = [(
            'fact_response',
            f'gid_{column_name}',
            'GID mappings check - key questions',
            'FAILED',
            qc_array,
            'Partial GID mapping fail for key questions',
            current_timestamp
        )]

    # Create DataFrame and append to table
    new_df = spark.createDataFrame(new_record, checks_table_schema)
    new_df.write.mode("append").saveAsTable(f"{silver_layer}.{canonical_tables_catalog}.canonical_qc_table")