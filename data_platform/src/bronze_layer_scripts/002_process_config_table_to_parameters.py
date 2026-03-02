# Databricks notebook source
df = spark.table('prism_bronze.data_plat_control.surveys')

# df.display()


# COMMAND ----------

from datetime import datetime

def convert_datetimes(d):
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in d.items()}

variables_to_process = [convert_datetimes(row.asDict()) for row in df.collect()]

dbutils.jobs.taskValues.set("survey_config", variables_to_process)