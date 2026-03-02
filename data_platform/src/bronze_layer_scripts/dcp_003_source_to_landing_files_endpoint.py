# Databricks notebook source
# MAGIC %pip install openpyxl==3.1.5

# COMMAND ----------

import requests
import json
import pandas as pd
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


# Get Datamap for Survey
url_file_list = f"https://prism.decipherinc.com/api/v1/surveys/selfserve/2ca6/{survey_id}/files"


# Requestion the json file
response = requests.get(url_file_list, headers=headers)
# Converting to json
file_list_json = response.json()

excel_file_list = [(file['filename'], file['modified']) for file in file_list_json if file['filename'] in ['Questions_mapping.csv', 'Variable_mapping.csv', 'Answers_mapping.csv', 'gold_mapping.csv']]


# COMMAND ----------

for raw_file_name, file_last_modified_dte in excel_file_list:
    print(f"\n{'='*80}")
    print(f"Processing file: {raw_file_name}")
    print(f"{'='*80}")
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_last_modified_timestamp = datetime.strptime(file_last_modified_dte, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
    print(f"Current timestamp: {timestamp}")
    print(f"File last modified: {file_last_modified_dte} (timestamp: {file_last_modified_timestamp})")

    local_path = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_files_endpoint/'
    local_path_csv = f'/Volumes/prism_bronze/landing_volumes/landing_files/sources/decipher/survey_files_endpoint_csv/'
    file_name = f'survey_file_{raw_file_name.split(".csv")[0]}_{survey_id}_{layout_id}_{timestamp}.csv'
    csv_file_name = file_name
    file_full_path = f'{local_path}{file_name}'
    csv_file_full_path = f'{local_path_csv}{csv_file_name}'
    
    print(f"\nFile paths:")
    print(f"  Excel path: {file_full_path}")
    print(f"  CSV path: {csv_file_full_path}")

    print(f"\nChecking for previously ingested files...")
    file_ingested_list = [f[0].split(".csv")[0].split("_")[-1] for f in dbutils.fs.ls(f"{local_path}") if f'{raw_file_name.split(".csv")[0]}_{survey_id}_{layout_id}' in f[0]] + [f[0].split(".xlsx")[0].split("_")[-1] for f in dbutils.fs.ls(f"{local_path}") if f'{raw_file_name.split(".xlsx")[0]}_{survey_id}_{layout_id}' in f[0]]
    print(f"  Found {len(file_ingested_list)} previously ingested file(s)")
    
    if len(file_ingested_list) > 0:
        file_last_ingested_timestamp = max(file_ingested_list)
        print(f"  Last ingested timestamp: {file_last_ingested_timestamp}")
    else:
        file_last_ingested_timestamp = '0'
        print(f"  No previous files found, setting last ingested timestamp to '0'")

    print(f"\nComparing timestamps:")
    print(f"  Last ingested: {file_last_ingested_timestamp}")
    print(f"  Last modified: {file_last_modified_timestamp}")
    
    if file_last_ingested_timestamp < file_last_modified_timestamp:
        print(f"  ✓ File needs to be downloaded (modified timestamp is newer)")
        
        url_file_dld = f"https://prism.decipherinc.com/api/v1/surveys/selfserve/2ca6/{survey_id}/files/{raw_file_name}"
        print(f"\nDownloading file from: {url_file_dld}")
        
        response = requests.get(url_file_dld, headers=headers)
        print(f"  Response status code: {response.status_code}")
        print(f"  Response content size: {len(response.content)} bytes")
        
        print(f"\nWriting file to: {file_full_path}")
        with open(file_full_path, 'wb') as file:
            file.write(response.content)
        print(f"  ✓ File written successfully")

        print(f"\nReading CSV file into DataFrame...")
        df = pd.read_csv(file_full_path)

        if raw_file_name == 'Variable_mapping.csv':
            # Drop row_number if it exists
            if 'row_number' in df.columns:
                df = df.drop(columns='row_number')
            
            # Rename id_survey_question to id_question if it exists
            if 'id_survey_question' in df.columns:
                df = df.rename(columns={'id_survey_question': 'id_question'})

        print(f"  DataFrame shape: {df.shape[0]} rows x {df.shape[1]} columns")
        
        print(f"\nSaving DataFrame to CSV: {csv_file_full_path}")
        df.to_csv(csv_file_full_path, quoting=1, index=False, encoding="utf-8")
        print(f"  ✓ CSV file saved successfully")
    else:
        print(f"  ✗ File skipped (already up to date)")
    
    print(f"{'='*80}\n")