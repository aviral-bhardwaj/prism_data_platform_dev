# Databricks notebook source
import configparser
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# Loading the config file
config_path = './config.ini'
config = configparser.ConfigParser()
config.read(config_path)


# COMMAND ----------

config_keys = [k for k in config.keys()]
config_keys.remove('DEFAULT')

col_names = ['survey_name']+[name for name in config[config_keys[0]].keys()]

# COMMAND ----------

list_of_surveys = []

for k in config_keys:
    info_list = [k]
    info_list.extend([i for i in config[k].values()])
    list_of_surveys.append(tuple(info_list))
# list_of_surveys

# COMMAND ----------

surveys_df = spark.createDataFrame(list_of_surveys, col_names)
surveys_df = surveys_df.withColumn('dte_created', F.current_timestamp())
surveys_df = surveys_df.withColumn('dte_updated', F.current_timestamp())

# surveys_df.display()

# COMMAND ----------

# Run this only for the first time
# surveys_df.write.mode("overwrite").saveAsTable("prism_bronze.data_plat_control.surveys")

# COMMAND ----------

target_delta_table = DeltaTable.forName(spark, "prism_bronze.data_plat_control.surveys")

columns_to_check = [c for c in surveys_df.columns if c not in ["survey_name", "dte_created", "dte_updated"]]

target_delta_table.alias("target").merge(
    surveys_df.alias("source"),
    "source.survey_name = target.survey_name"
).whenMatchedUpdate(
    condition=" OR ".join([
        f"source.{col} IS DISTINCT FROM target.{col}" for col in columns_to_check
    ]),
    set={
        **{c: f"source.{c}" for c in surveys_df.columns if c != "dte_created"}
    }
).whenNotMatchedInsertAll(
).execute()