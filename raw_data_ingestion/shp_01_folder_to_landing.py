# Databricks notebook source
#imports
from urllib.parse import quote
from datetime import datetime
import requests
from io import BytesIO
import configparser

# COMMAND ----------

# Loading the config file
config_path = '/Volumes/prism_bronze/landing_volumes/landing_files/config.ini'
config = configparser.ConfigParser()
config.read(config_path)

# COMMAND ----------

#getting the values from config file
instrument_name = config['us_qsr']['instrument_name']
phase = config['us_qsr']['phase']
source_file_name = config['us_qsr']['source_file_name']
survey_id = config['us_qsr']['survey_id']
wave = config['us_qsr']['wave']
source_sharepoint_folder = config['us_qsr']['source_sharepoint_folder']
target_path = config['us_qsr']['target_path']
file_name = config['us_qsr']['source_file_name']

# COMMAND ----------

# === CONFIGURATION ===
client_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-id")
client_secret = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-secret")
tenant_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-tenant-id")


#source
sharepoint_domain = "bainandcompany.sharepoint.com"
site_name = "GlobalPrism"
sharepoint_folder = f"{source_sharepoint_folder}/{instrument_name}_{survey_id}/{wave}"

#target 
target_path = f'{target_path}/{instrument_name}_{survey_id}/{wave}/{phase}/'

date_cutoff = datetime(2025, 5, 5)


graph_url = "https://graph.microsoft.com/v1.0"

# === AUTH ===
def get_graph_token():
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }
    return requests.post(url, data=data).json()["access_token"]

access_token = get_graph_token()
headers = {"Authorization": f"Bearer {access_token}"}

# === IDENTIFIERS ===
def get_site_id():
    url = f"{graph_url}/sites/{sharepoint_domain}:/sites/{site_name}"
    return requests.get(url, headers=headers).json()["id"]

def get_drive_id(site_id):
    url = f"{graph_url}/sites/{site_id}/drives"
    res = requests.get(url, headers=headers)
    for drive in res.json()["value"]:
        if drive["name"] == "Documents":
            return drive["id"]

site_id = get_site_id()
drive_id = get_drive_id(site_id)

# === FILE OPERATION ===
def parse_graph_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ")

def fetch_and_upload_file():
    encoded_path = quote(sharepoint_folder)
    url = f"{graph_url}/drives/{drive_id}/root:/{encoded_path}:/children"
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise Exception(f"Failed to list SharePoint folder: {res.text}")

    items = res.json().get("value", [])
    target_file = next((item for item in items if item["name"] == file_name), None)

    if not target_file:
        raise FileNotFoundError(f"File '{file_name}' not found in SharePoint folder.")

    modified_time = parse_graph_datetime(target_file["lastModifiedDateTime"])
    if modified_time < date_cutoff:
        raise Exception(f"File is too old (last modified: {modified_time})")

    download_url = target_file["@microsoft.graph.downloadUrl"]
    print(f"⬇️ Downloading: {file_name}")
    file_content = requests.get(download_url).content


    # Saving the file to local storage
    dbutils.fs.mkdirs(target_path)
    with open(f"{target_path}{file_name}", "wb") as f:
        f.write(file_content)
    print(f"✅ Upload complete: {target_path}/{file_name}")

# === EXECUTE ===
fetch_and_upload_file()