# Databricks notebook source
import requests
import json
from datetime import datetime
from pyspark import pandas as ps

# COMMAND ----------

# getting api token from Azure Key Vault
api_key = dbutils.secrets.get('prism-dl-scope', 'decypher-api-verta-token')
# Creating Header to request API
headers = {
    "x-apikey": f"{api_key}",
    "Content-Type": "application/json"
    }

# COMMAND ----------

# Get list of Surveys
url_surveys = f"https://prism.decipherinc.com/api/v1/rh/companies/all/surveys?format=json"

# Requestion the json file
response_surveys = requests.get(url_surveys, headers=headers)
# Converting to json
response_surveys_json = response_surveys.json()


# COMMAND ----------

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
local_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/all_surveys_endpoint/'
file_name = f'all_surveys_{timestamp}.json'

# COMMAND ----------

# dbutils.fs.mkdirs(local_path)

# Saving the file to local storage
with open(f"{local_path}{file_name}", "w") as f:
    json.dump(response_surveys_json, f, indent=4)