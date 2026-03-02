# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Ingesting layout raw response data from source (Decipher) and store on landing (ADL)

# COMMAND ----------

#Creating Widgets
dbutils.widgets.text('instrument_name', '')
dbutils.widgets.text('survey_id', '')
dbutils.widgets.text('wave', '')
dbutils.widgets.text('agg_suffix', '')
dbutils.widgets.text('layout_id', '')

# COMMAND ----------

import requests
import json

# COMMAND ----------

#Collecting widget values
survey_id = dbutils.widgets.get('survey_id')
instrument_name = dbutils.widgets.get('instrument_name').lower()
wave = dbutils.widgets.get('wave').lower()
agg_suffix = dbutils.widgets.get('agg_suffix').lower() 
layout = dbutils.widgets.get('layout_id') 

# COMMAND ----------

# getting api token from Azure Key Vault
api_key = dbutils.secrets.get('prism-dl-scope', 'decypher-api-verta-token')
# Creating Header to request API
headers = {
    "x-apikey": f"{api_key}",
    "Content-Type": "application/json"
    }

# COMMAND ----------

# Get Datamap for Survey
url_datamap = f"https://prism.decipherinc.com/api/v1/surveys/selfserve/2ca6/{survey_id}/data?format=json&layout={layout}"

# Requestion the json file
response = requests.get(url_datamap, headers=headers)
# Converting to json
response_json = response.json()

# COMMAND ----------

# Creating path to save and move file checking if there is agg_suffix
if agg_suffix == 0 or agg_suffix == '' or agg_suffix == '0':
    local_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'
    file_name = f'dcp_response.json'
    # path_to_save = f'abfss://020-decypher-raw-data@npsprismdplatformdldev.dfs.core.windows.net/landing_layer/{instrument_name}/{wave_quarter}/datamap_{layout}.json'
else:
    local_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/{instrument_name}/{wave}/'
    file_name = f'dcp_response_{agg_suffix}.json'
    # path_to_save = f'abfss://020-decypher-raw-data@npsprismdplatformdldev.dfs.core.windows.net/landing_layer/{instrument_name}/{wave_quarter}/datamap_{agg_suffix}_{layout}.json'

# COMMAND ----------

dbutils.fs.mkdirs(local_path)

# Saving the file to local storage
with open(f"{local_path}{file_name}", "w") as f:
    json.dump(response_json, f, indent=4)