# Databricks notebook source
import requests
import json

# COMMAND ----------

survey_id = '250322'# '250123' 

# COMMAND ----------

# getting api token from Azure Key Vault
api_key = dbutils.secrets.get('prism-dl-scope', 'decypher-api-verta-token')
# Creating Header to request API
headers = {
    "x-apikey": f"{api_key}",
    "Content-Type": "application/json"
    }

# COMMAND ----------

# Get layouts for Survey
url_layouts = f"https://prism.decipherinc.com/api/v1/surveys/selfserve/2ca6/{survey_id}/layouts?format=json"

# Requestion the json file
response_layouts = requests.get(url_layouts, headers=headers)
# Converting to json
response_layouts_json = response_layouts.json()

df = spark.createDataFrame(response_layouts_json)
df.select(['createdBy',
 'createdOn',
 'description',
 'id',
 'updatedBy',
 'updatedOn',]).display()

# COMMAND ----------

import json
with open("/Volumes/prism_bronze/landing_volumes/landing_files/us_qsr/25q1/phase/layout_metadata/layout_20250515125744.json", "r") as f:
    dat = json.load(f)
    spark.createDataFrame(dat).display()

# COMMAND ----------

# Get list of Surveys
url_surveys = f"https://prism.decipherinc.com/api/v1/rh/companies/all/surveys?format=json"


# Requestion the json file
response_surveys = requests.get(url_surveys, headers=headers)
# Converting to json
response_surveys_json = response_surveys.json()

from pyspark import pandas as ps

df_survey=ps.DataFrame(response_surveys_json, dtype=str)

sdf_survey = df_survey.to_spark()
sdf_survey.display()