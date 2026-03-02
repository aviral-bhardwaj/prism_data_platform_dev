import os
import urllib.parse

import msal
import requests


class GraphAuth:
    def __init__(self, client_id: str, tenant_id: str, client_secret: str, scopes=None):
        """
        Initializes the GraphAuth object with the provided credentials
        and acquires an access token using MSAL.
        """
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.scopes = scopes or ["https://graph.microsoft.com/.default"]
        self.app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret
        )
        self.access_token = None

    def acquire_token(self) -> str:
        """
        Acquires an access token from Microsoft Graph.
        Raises an Exception if token retrieval fails.
        """
        token_response = self.app.acquire_token_for_client(scopes=self.scopes)

        if "access_token" in token_response:
            self.access_token = token_response["access_token"]
            return self.access_token
        else:
            raise Exception("Could not obtain access token", token_response.get("error_description"))


class SharePointClient:
    def __init__(self, site_id: str, drive_id: str, base_folder_id: str, access_token: str):
        self.site_id = site_id
        self.drive_id = drive_id
        self.base_folder_id = base_folder_id
        self.access_token = access_token
        self.resource_url = "https://graph.microsoft.com/"

    def _headers(self, content_type: str = "application/json") -> dict:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": content_type
        }

    def get_or_create_folder(self, folder_path: str) -> str:
        """
        Ensures the folder path exists. Creates any missing folders along the path.
        Returns the ID of the final folder.
        """
        path_parts = folder_path.strip("/").split("/")
        current_folder_id = self.base_folder_id

        for part in path_parts:
            encoded_part = urllib.parse.quote(part.replace("'", "''"))
            get_url = (f"{self.resource_url}v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/"
                       f"{current_folder_id}/children?$filter=name eq '{encoded_part}'")
            response = requests.get(get_url, headers=self._headers())

            if response.status_code == 200:
                items = response.json().get("value", [])
                matching_item = next(
                    (item for item in items if item.get("name", "").lower() == part.lower() and "folder" in item),
                    None
                )
                if matching_item:
                    current_folder_id = matching_item.get("id")
                    continue
                else:
                    create_url = (f"{self.resource_url}v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/"
                                  f"{current_folder_id}/children")
                    data = {
                        "name": part,
                        "folder": {},
                        "@microsoft.graph.conflictBehavior": "rename"
                    }
                    create_resp = requests.post(create_url, headers=self._headers(), json=data)
                    if create_resp.status_code in [200, 201]:
                        folder_info = create_resp.json()
                        current_folder_id = folder_info.get("id")
                    else:
                        raise Exception(f"Error creating folder '{part}': {create_resp.status_code} {create_resp.text}")
            else:
                raise Exception(f"Error retrieving folder '{part}': {response.status_code} {response.text}")

        return current_folder_id

    def upload_file_simple(self, dynamic_folder: str, filename: str, file_content: bytes) -> dict:
        """
        Uploads a file to SharePoint under the specified dynamic folder.
        """
        final_folder_id = self.get_or_create_folder(dynamic_folder)
        encoded_filename = urllib.parse.quote(filename)

        upload_url = (f"{self.resource_url}v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/"
                      f"{final_folder_id}:/{encoded_filename}:/content")

        response = requests.put(upload_url, headers=self._headers("application/octet-stream"), data=file_content)

        if response.status_code in [200, 201]:
            file_info = response.json()
            # file_url = file_info.get("webUrl", "")
            download_url = file_info.get("@microsoft.graph.downloadUrl", "")
            return download_url
        else:
            raise Exception(f"File upload failed: {response.status_code} {response.text}")

    def upload_file_chunked(self, dynamic_folder: str, filename: str, file_path: str, chunk_size: int = 5 * 1024 * 1024) -> dict:
        """
        Uploads a file in chunks using an upload session for large files.
        """
        final_folder_id = self.get_or_create_folder(dynamic_folder)
        encoded_filename = urllib.parse.quote(filename)

        session_url = (f"{self.resource_url}v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/"
                       f"{final_folder_id}:/{encoded_filename}:/createUploadSession")
        session_resp = requests.post(session_url, headers=self._headers(), json={})

        if session_resp.status_code not in [200, 201]:
            raise Exception(f"Error creating upload session: {session_resp.status_code} {session_resp.text}")

        upload_url = session_resp.json().get("uploadUrl")
        if not upload_url:
            raise Exception("Upload session URL not provided.")

        file_size = os.path.getsize(os.path.join(file_path, filename))

        with open(os.path.join(file_path, filename), "rb") as f:
            start = 0
            while start < file_size:
                chunk_data = f.read(chunk_size)
                end = start + len(chunk_data) - 1
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Length": str(len(chunk_data)),
                    "Content-Range": f"bytes {start}-{end}/{file_size}"
                }
                chunk_resp = requests.put(upload_url, headers=headers, data=chunk_data)
                if chunk_resp.status_code not in [200, 201, 202]:
                    raise Exception(f"Chunk upload failed: {chunk_resp.status_code} {chunk_resp.text}")
                start = end + 1

        file_info = chunk_resp.json()
        # file_url = file_info.get("webUrl", "")
        download_url = file_info.get("@microsoft.graph.downloadUrl", "")
        return download_url
        # return output_logger

    def upload_file(self, dynamic_folder: str, filename: str, file_path: str, size_threshold: int = 4 * 1024 * 1024) -> dict:
        """
        Chooses the appropriate upload method based on file size.
        """
        file_size = os.path.getsize(os.path.join(file_path, filename))
        print(f"File size: {file_size} bytes")

        if file_size <= size_threshold:
            with open(os.path.join(file_path, filename), "rb") as f:
                file_bytes = f.read()
            return self.upload_file_simple(dynamic_folder, filename, file_bytes)
        else:
            return self.upload_file_chunked(dynamic_folder, filename, file_path)

    def list_subfolders(self, parent_folder_id):
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{parent_folder_id}/children"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            items = response.json().get("value", [])

            folders = [
                {
                    "name": item["name"],
                    "folder_id": item["id"],
                    "webUrl": item.get("webUrl", "")
                }
                for item in items if "folder" in item
            ]

            print("📁 Subfolders:")
            for folder in folders:
                print(f"✅ {folder['name']} → {folder['folder_id']}")

            return folders

        else:
            print(f"❌ Failed to list subfolders: {response.status_code} - {response.text}")
            return None

    def get_parent_folder_id(self, subfolder_id):
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{subfolder_id}"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            parent_id = data.get("parentReference", {}).get("id")
            parent_path = data.get("parentReference", {}).get("path")

            print(f"✅ Parent Folder ID: {parent_id}")
            print(f"📂 Parent Path: {parent_path}")
            return parent_id
        else:
            print(f"❌ Failed to get parent folder: {response.status_code} - {response.text}")
            return None


def upload_to_sharepoint(dbutils, dynamic_folder: str, filename: str, file_path: str, size_threshold: int = 4 * 1024 * 1024) -> dict:
    """
    This function encapsulates the whole process: authenticates, acquires the token,
    and uploads the file (either simple or chunked) to SharePoint.
    """
    # Retrieve secrets using Databricks' dbutils (assumed to be available in your environment) dbutils.secrets.get('prism-dl-scope', 'decypher-api-verta-token')
    client_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-id")
    tenant_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-tenant-id")
    client_secret = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-secret")
    site_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-site-id")
    drive_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-drive-id")
    base_folder_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-folder-id")

    # Authenticate and acquire token
    auth = GraphAuth(client_id, tenant_id, client_secret)
    access_token = auth.acquire_token()

    # Create SharePoint client instance
    sharepoint_client = SharePointClient(site_id, drive_id, base_folder_id, access_token)

    # Upload file (based on size threshold)
    return sharepoint_client.upload_file(dynamic_folder, filename, file_path, size_threshold)


def list_sub_folders_sharepoint(dbutils, parent_folder_id):
    client_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-id")
    tenant_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-tenant-id")
    client_secret = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-secret")
    site_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-site-id")
    drive_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-drive-id")
    base_folder_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-folder-id")

    # Authenticate and acquire token
    auth = GraphAuth(client_id, tenant_id, client_secret)
    access_token = auth.acquire_token()

    # Create SharePoint client instance
    sharepoint_client = SharePointClient(site_id, drive_id, base_folder_id, access_token)

    return sharepoint_client.list_subfolders(parent_folder_id)


def list_parent_folders_sharepoint(dbutils, sub_folder_id):
    client_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-id")
    tenant_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-tenant-id")
    client_secret = dbutils.secrets.get(scope="prism-dl-scope", key="shp-client-secret")
    site_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-site-id")
    drive_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-drive-id")
    base_folder_id = dbutils.secrets.get(scope="prism-dl-scope", key="shp-folder-id")

    # Authenticate and acquire token
    auth = GraphAuth(client_id, tenant_id, client_secret)
    access_token = auth.acquire_token()

    # Create SharePoint client instance
    sharepoint_client = SharePointClient(site_id, drive_id, base_folder_id, access_token)

    return sharepoint_client.get_parent_folder_id(sub_folder_id)
