# Databricks notebook source
from delta.tables import DeltaTable


# COMMAND ----------

# Create updates DataFrame
updates_data = [
    ('250810', 'true'),
    ('251039', 'true'),
    ('251024', 'true'),
    ('251046', 'true')
]

updates_df = spark.createDataFrame(updates_data, ['survey_id', 'active'])

# Merge approach (more efficient for bulk updates)

delta_table = DeltaTable.forName(spark, "prism_silver.canonical_tables.dim_wave")

delta_table.alias("target").merge(
    updates_df.alias("updates"),
    "target.survey_id = updates.survey_id"
).whenMatchedUpdate(
    set={"active": "updates.active"}
).execute()
