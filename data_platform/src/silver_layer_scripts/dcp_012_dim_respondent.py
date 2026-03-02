# Databricks notebook source
dbutils.widgets.text("jobrunid", "")
job_runid = dbutils.widgets.get("jobrunid")

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
#from data_plat_cdc_logic import *
from data_plat_cdc_logic_updated import *

# COMMAND ----------

# For running on serverless
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC `Variables`
# MAGIC

# COMMAND ----------

# DBTITLE 1,Variables
# Setting up variables for data layers
silver_layer = 'prism_silver'
bronze_layer = 'prism_bronze'

# Catalog and table names
canonical_tables_catalog = "canonical_tables"

dim_wave_table = "dim_wave"  # Dimension table for wave metadata
stg_survey_responses_long_format_table = "stg_survey_responses_long_format"  # Staging table for survey responses (long format)
dim_respondent_table = "dim_respondent"  # Respondent dimension table
dim_question_table = "dim_question"  # Dimension table for questions

# List of universal question IDs to filter on
question_filter = [
    "uid_Age",
    "uid_Age_Group",
    "uid_Gender",
    "uid_qualified_status",
    "uid_sampling_phase",
    "uid_termination_code",
    "uid_termination_code_OG",
    "uid_HH_Income",
    "uid_State",
    "uid_Zip_code",
    "uid_Region",
    "uid_quality_start",
    "uid_survey_start_date",
    "uid_survey_end_date",
    "uid_sample_vendor"
]

# COMMAND ----------

# MAGIC %md
# MAGIC `Sources`

# COMMAND ----------

## wave table
df_wave = spark.table(f'{silver_layer}.{canonical_tables_catalog}.{dim_wave_table}').where(F.col('active')==True).select('gid_instrument','gid_wave','survey_id', 'layout_id')

# Adding filter to account for existing data
df_survey_metadata = spark.sql("select distinct survey_id, layout_id from prism_bronze.decipher.survey_metadata")

df_wave = df_wave.join(df_survey_metadata, on=['survey_id', 'layout_id'], how='inner').drop('layout_id')

raw_data = spark.table(f'{silver_layer}.{canonical_tables_catalog}.{stg_survey_responses_long_format_table}').where(F.col('deleted_in_source')==False)

## dim_question table
df_question = spark.table(f'{silver_layer}.{canonical_tables_catalog}.{dim_question_table}').select("gid_instrument","id_survey_question",'id_universal_question')

# COMMAND ----------

# MAGIC %md
# MAGIC `Joining with dim_wave to get Active records`

# COMMAND ----------

# Join raw_data with df_wave on 'survey_id' to enrich with wave metadata, then drop duplicate survey_id column
df = raw_data.join(
    df_wave, 
    on=['survey_id'], 
    how='inner'
).drop(df_wave.survey_id)


# COMMAND ----------

# MAGIC %md
# MAGIC `Filteration of Respondent Questions`

# COMMAND ----------

# Filter df_question to only include rows where id_universal_question is in question_filter
df_question = df_question.filter(
    F.col("id_universal_question").isin(question_filter)
)

# COMMAND ----------

df = df.join(df_question,on=['gid_instrument','id_survey_question'],how='inner')
df = df.select('gid_instrument','gid_wave','uuid','id_universal_question','txt_answer')


# COMMAND ----------

# MAGIC %md
# MAGIC `Fields Creation`

# COMMAND ----------

# Pivot the DataFrame so each id_universal_question becomes a column, with the first txt_answer as the value
df = df.groupBy("uuid", "gid_instrument", "gid_wave").pivot("id_universal_question").agg(F.first("txt_answer"))

# COMMAND ----------

# MAGIC %md
# MAGIC `Renaming of fields according to table`

# COMMAND ----------

# Convert all column names in df to lowercase
df = df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

# Rename columns to match target schema
df = df.withColumnsRenamed({
       'uuid': 'id_respondent',
       'uid_age': 'num_age',
       'uid_age_group': 'age_range',
       'uid_gender': 'gender',
       'uid_hh_income': 'income_range',
       'uid_region': 'region',
       'uid_state': 'state',
       'uid_zip_code': 'zip_code',
       'uid_qualified_status': 'qualified_status',
       'uid_sampling_phase': 'sampling_phase',
       'uid_termination_code': 'termination_code_ed',
       "uid_termination_code_og": 'termination_code',
       "uid_sampling_phase": "sampling_phase",
       "uid_quality_start": "quality_start",
       "uid_survey_start_date": 'start_date',
       "uid_survey_end_date": 'end_date',
       "uid_sample_vendor": 'sample_vendor'
       })

# COMMAND ----------

# MAGIC %md
# MAGIC `Final Selection`

# COMMAND ----------

# Convert 'start_date' and 'end_date' columns to date type using the specified format
df = df.withColumn('start_date', F.to_date('start_date', 'MM/dd/yyyy HH:mm')).withColumn('end_date', F.to_date('end_date', 'MM/dd/yyyy HH:mm')).withColumn('dte_create', F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Final Selection
from pyspark.sql import functions as F

# 1) Read target schema (no data)
target_df = spark.table(f"{silver_layer}.{canonical_tables_catalog}.{dim_respondent_table}").limit(0)

target_fields = {f.name: f.dataType for f in target_df.schema.fields}
target_fields.pop("gid_respondent", None)
df_fields = {f.name: f.dataType for f in df.schema.fields}

# 2) Missing columns
missing_cols = [c for c in target_fields.keys() if c not in df_fields]

# 3) Add missing columns with NULL casted to target datatype
for c in missing_cols:
    df = df.withColumn(c, F.lit(None).cast(target_fields[c]))

# 4) (Optional but useful) Reorder columns exactly like target table
select_cols = list(target_fields.keys())
final_df = df.select(*select_cols).drop_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC `Data type change and Null conversion`

# COMMAND ----------

# DBTITLE 1,CDC Implementation
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce

target_table = f"{silver_layer}.{canonical_tables_catalog}.{dim_respondent_table}"
key_cols = ["gid_instrument", "gid_wave", "id_respondent"]

print("============================================")
print("SCD / MERGE begins for :", target_table)
print("Key columns:", key_cols)
print("============================================\n")

# ----------------------------
# Clean + normalize incoming df
# ----------------------------
print("Step 1: Cleaning source dataframe begins")

final_df = final_df.withColumn("num_age", F.col("num_age").cast("int"))
final_df = final_df.select(
    [F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in final_df.columns]
)

print("Step 1: Cleaning source dataframe completed\n")

# ---------------------------------------
# If table exists -> MERGE (upsert + audit)
# ---------------------------------------
if spark.catalog.tableExists(target_table):

    print("Target table exists:", target_table)
    print("Reading target table begins")

    tgt_df = spark.table(target_table)
    target_cols = tgt_df.columns

    print("Reading target table completed")
    print("Target columns count:", len(target_cols), "\n")

    print("Preparing source dataframe for merge begins")
    src = (
        final_df
        .withColumn("dte_update", F.current_timestamp())
        .withColumn("jobrunid", F.lit(job_runid))
    )
    print("Preparing source dataframe for merge completed\n")

    # ----------------------------
    # OPTIONAL: CDC-like hash check
    # (to print updated/new counts like your CDC code)
    # ----------------------------
    exclusion_cols = {"dte_create", "dte_update", "jobrunid"}  # treat as audit
    comparable_cols = [c for c in src.columns if c in target_cols and c not in exclusion_cols and c not in key_cols]

    if len(comparable_cols) > 0:
        print("Hashvalue creation for src table begins")
        src_h = src.withColumn(
            "hashvalue",
            F.sha2(F.concat_ws(" ", *[F.col(c).cast("string") for c in comparable_cols]), 256)
        )
        print("Hashvalue creation for src table completed\n")

        print("Hashvalue creation for tgt table begins")
        tgt_h = tgt_df.withColumn(
            "hashvalue",
            F.sha2(F.concat_ws(" ", *[F.col(c).cast("string") for c in comparable_cols]), 256)
        )
        print("Hashvalue creation for tgt table completed\n")

        # Join condition (null-safe) like your CDC
        join_cond = reduce(lambda a, b: a & b, [src_h[k].eqNullSafe(tgt_h[k]) for k in key_cols])

        print("Checking updated records begins")
        df_updated_keys = (
            src_h.join(tgt_h, join_cond, "inner")
                 .where(src_h["hashvalue"] != tgt_h["hashvalue"])
                 .select(*[src_h[k].alias(k) for k in key_cols])
                 .dropDuplicates()
        )
        updated_cnt = df_updated_keys.count()
        print(f"Updated records found: {updated_cnt}\n")

        print("Checking new records begins")
        df_new_keys = (
            src_h.join(tgt_h, join_cond, "left_anti")
                 .select(*[src_h[k].alias(k) for k in key_cols])
                 .dropDuplicates()
        )
        new_cnt = df_new_keys.count()
        print(f"New records found: {new_cnt}\n")

    else:
        print("Hash check skipped: no comparable (non-key, non-audit) columns found.\n")

    # ----------------------------
    # Build merge condition (NULL-safe)
    # ----------------------------
    merge_condition = " AND ".join([f"t.`{c}` <=> s.`{c}`" for c in key_cols])

    # --------
    # UPDATE SET
    # --------
    print("Preparing update expression begins")

    update_set = {}
    for c in target_cols:
        if c == "dte_create":
            continue
        if c in src.columns:
            update_set[c] = f"s.`{c}`"

    # Force audit fields on update
    if "dte_update" in target_cols:
        update_set["dte_update"] = "current_timestamp()"
    if "jobrunid" in target_cols:
        update_set["jobrunid"] = f"'{job_runid}'"

    # If gid_respondent exists in target, don't overwrite it
    if "gid_respondent" in update_set:
        update_set.pop("gid_respondent")

    print("Preparing update expression completed\n")

    # --------
    # INSERT VALUES
    # --------
    print("Preparing insert expression begins")

    insert_values = {}
    for c in target_cols:
        if c in src.columns:
            insert_values[c] = f"s.`{c}`"
        else:
            if c == "dte_create":
                insert_values[c] = "current_timestamp()"
            elif c == "dte_update":
                insert_values[c] = "current_timestamp()"
            elif c == "jobrunid":
                insert_values[c] = f"'{job_runid}'"
            else:
                insert_values[c] = "NULL"

    print("Preparing insert expression completed\n")

    # ----------------------------
    # Execute merge
    # ----------------------------
    print("Merge for table started :", target_table)

    tgt = DeltaTable.forName(spark, target_table)

    (tgt.alias("t")
        .merge(src.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    print("Merge completed for:", target_table, "\n")

else:
    # ---------------------------
    # Else -> create new Delta table
    # ---------------------------
    print("Target table does NOT exist. Creating new table:", target_table)

    final_df = (
        final_df
        .withColumn("dte_create", F.current_timestamp())
        .withColumn("dte_update", F.current_timestamp())
        .withColumn("jobrunid", F.lit(job_runid))
    )

    print("Adding gid_respondent begins")
    final_df = final_df.withColumn(
        "gid_respondent",
        F.row_number().over(Window.orderBy(*key_cols))
    )
    print("Adding gid_respondent completed\n")

    print("Writing new Delta table started:", target_table)
    (final_df
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
    print("Writing new Delta table completed:", target_table, "\n")

print("============================================")
print("SCD / MERGE finished for :", target_table)
print("============================================")


# COMMAND ----------

# MAGIC %md
# MAGIC `Table Creation with CDF Enabled`

# COMMAND ----------

# DBTITLE 1,Table Creation with CDF Enabled
# %sql
# -- Create dim_respondent Table in prism_silver.default with Change Data Feed Enabled
# CREATE TABLE IF NOT EXISTS prism_silver.canonical_tables.dim_respondent ( 
#   gid_respondent INTEGER,
#   gid_instrument INTEGER,
#   gid_wave INTEGER, 
#   id_respondent STRING,
#   num_age INTEGER,
#   age_range STRING,
#   gender STRING,
#   income_range STRING,
#   region STRING,
#   state STRING,
#   zip_code STRING,
#   qualified_status STRING,   --- need to check column name
#   sampling_phase STRING,
#   termination_code STRING,
#   termination_code_ed STRING,
#   dte_create TIMESTAMP,
#   dte_update TIMESTAMP
#  )
#   TBLPROPERTIES (
#     delta.enableChangeDataFeed = true
# );