# onedrive_to_bigquery.py - Lists folders/files to help find paths

import msal
import requests
import io
import os
from google.cloud import bigquery

CLIENT_ID = "d1adc91d-1026-4c7b-8ce9-ddc051dced98"

REFRESH_TOKEN = os.getenv("ONEDRIVE_REFRESH_TOKEN")
if not REFRESH_TOKEN:
    raise ValueError("Set ONEDRIVE_REFRESH_TOKEN with single quotes!")

SCOPES = ["https://graph.microsoft.com/Files.Read.All", "https://graph.microsoft.com/User.Read"]

AUTHORITY = "https://login.microsoftonline.com/consumers"

# START HERE: Set to "" or "/" to list top-level (root/"My files")
FOLDER_PATH = "/"  # Change later to "/test_folder" once confirmed

BQ_PROJECT = "forward-liberty-445306-t6"
BQ_DATASET = "streaming_ds"
BQ_TABLE = "table"

# Get token
app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
token_result = app.acquire_token_by_refresh_token(REFRESH_TOKEN, scopes=SCOPES)
if "access_token" not in token_result:
    raise ValueError(f"Token error: {token_result.get('error_description')}")

headers = {"Authorization": f"Bearer {token_result['access_token']}"}
print("Access token OK.")

# Build correct URL
if FOLDER_PATH.strip() in ["", "/"]:
    folder_url = "https://graph.microsoft.com/v1.0/me/drive/root/children"
    folder_display = "root (My files)"
else:
    folder_url = f"https://graph.microsoft.com/v1.0/me/drive/root:{FOLDER_PATH}:/children"
    folder_display = FOLDER_PATH.lstrip("/")

response = requests.get(folder_url, headers=headers)
if response.status_code != 200:
    raise ValueError(f"List error: {response.status_code} {response.text}")

items = response.json().get("value", [])
print(f"\nFound {len(items)} items in '{folder_display}':")
for item in items:
    item_type = "Folder" if "folder" in item else "File"
    print(f"  - [{item_type}] {item['name']}")

# BigQuery load (only CSVs in the selected folder)
bq_client = bigquery.Client(project=BQ_PROJECT)
csv_count = 0
for item in items:
    if item.get("file") and item["name"].lower().endswith(".csv"):
        csv_count += 1
        print(f"\nProcessing: {item['name']}")
        download_url = item["@microsoft.graph.downloadUrl"]
        content = requests.get(download_url).content

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        load_job = bq_client.load_table_from_file(
            io.BytesIO(content),
            f"{BQ_DATASET}.{BQ_TABLE}",
            job_config=job_config
        )
        load_job.result()
        print(f"✓ Loaded {item['name']}")

if csv_count == 0:
    print("\nNo CSVs found in this folder.")
print("\nTo sync a subfolder, set FOLDER_PATH = '/exact_folder_name' (case-sensitive).")