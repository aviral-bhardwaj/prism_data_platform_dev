# Databricks notebook source
import requests
import json
from datetime import datetime

# COMMAND ----------

# getting api token from Azure Key Vault
api_key = dbutils.secrets.get('prism-dl-scope', 'decypher-api-verta-token')
# Creating Header to request API
headers = {
    "x-apikey": f"{api_key}",
    "Content-Type": "application/json"
    }

# COMMAND ----------

# config_string = '{"phase":"main","industry":"hotel","survey_name":"NPS_Prism_US_Hotels_2025_Q2_Main","survey_provider":"decipher","geolocation":"us","wave":"25q2","layout_id":"24239","dte_created":"2025-07-10T16:36:24.678119","dte_updated":"2025-07-10T16:36:24.678119","survey_id":"777777"}'

# config_string = '{"phase":"saucy_boost","industry":"qsr","survey_name":"NPS_Prism_Quick_Serve_Restaurant_2025_Q1_Main_Boost","survey_provider":"decipher","geolocation":"us","wave":"25q1","layout_id":"24235","dte_created":"2025-07-10T16:36:24.678119","dte_updated":"2025-07-10T16:36:24.678119","survey_id":"250315"}'

# config_string = '{"phase":"main","industry":"qsr","survey_name":"NPS_Prism_Quick_Serve_Restaurant_2025_Q1_Main","survey_provider":"decipher","geolocation":"us","wave":"25q1","layout_id":"24235","dte_created":"2025-07-10T16:36:24.678119","dte_updated":"2025-07-10T16:36:24.678119","survey_id":"250123"}'

# COMMAND ----------

config_string = dbutils.widgets.get("config_input")
config_dict = eval(config_string)

survey_id = config_dict['survey_id']
layout_id = config_dict['layout_id']

# Get Responses for Survey
url_responses = f"https://prism.decipherinc.com/api/v1/surveys/selfserve/2ca6/{survey_id}/data?format=json&layout={layout_id}&cond=everything"

# Requestion the json file
response = requests.get(url_responses, headers=headers)
# Converting to json
response_json = response.json()


# COMMAND ----------

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
local_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_responses_endpoint/{survey_id}_{layout_id}/'
file_name = f'survey_responses_{survey_id}_{layout_id}_{timestamp}.json'

# COMMAND ----------

dbutils.fs.mkdirs(local_path)

# Saving the file to local storage
with open(f"{local_path}{file_name}", "w") as f:
    json.dump(response_json, f, indent=4)