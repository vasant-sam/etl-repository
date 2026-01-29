# onedrive_to_bigquery.py - Handles CSV (to existing table) and JSON (parse, create new table, insert records)

import msal
import requests
import io
import os
import json  # For JSON parsing
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# === YOUR CONFIGURATION ===
CLIENT_ID = "d1adc91d-1026-4c7b-8ce9-ddc051dced98"

REFRESH_TOKEN = os.getenv("ONEDRIVE_REFRESH_TOKEN")

if not REFRESH_TOKEN:
    raise ValueError(
        "Error: ONEDRIVE_REFRESH_TOKEN is not set!\n"
        "Run: export ONEDRIVE_REFRESH_TOKEN='your-token' (with single quotes)"
    )

SCOPES = [
    "https://graph.microsoft.com/Files.Read.All",
    "https://graph.microsoft.com/User.Read"
]

AUTHORITY = "https://login.microsoftonline.com/consumers"

# Folder to sync — use "" or "/" for root
FOLDER_PATH = "/"  # Change to "/test_folder" if needed

BQ_PROJECT = "forward-liberty-445306-t6"
BQ_DATASET = "streaming_ds"
CSV_TABLE = "table"  # Existing table for CSVs
JSON_TABLE = "employees"  # New table for JSON records

# === Get access token ===
app = msal.PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)
token_result = app.acquire_token_by_refresh_token(refresh_token=REFRESH_TOKEN, scopes=SCOPES)

if "access_token" not in token_result:
    raise ValueError(f"Token error: {token_result.get('error_description')}")

access_token = token_result["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}
print("Access token obtained successfully.")

# === List files in folder ===
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

# === BigQuery client ===
bq_client = bigquery.Client(project=BQ_PROJECT)

# === Process files ===
csv_count = 0
json_count = 0
for item in items:
    if item.get("file"):
        file_name = item["name"].lower()
        if file_name.endswith(".csv") or file_name.endswith(".json"):
            print(f"\nProcessing: {item['name']}")

            download_url = item["@microsoft.graph.downloadUrl"]
            file_response = requests.get(download_url)
            file_response.raise_for_status()
            content = file_response.content

            if file_name.endswith(".csv"):
                # CSV handling - no changes, load to existing table
                csv_count += 1
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    autodetect=True,
                    skip_leading_rows=1,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                table_ref = f"{BQ_DATASET}.{CSV_TABLE}"
                load_job = bq_client.load_table_from_file(
                    io.BytesIO(content),
                    table_ref,
                    job_config=job_config
                )
                load_job.result()
                print(f"✓ Loaded CSV {item['name']} into {table_ref}")

            elif file_name.endswith(".json"):
                # JSON handling: Parse, create table if not exists, insert records
                json_count += 1
                # Parse JSON (assume NDJSON or array of objects)
                try:
                    lines = content.decode('utf-8').splitlines()
                    records = []
                    for line in lines:
                        if line.strip():
                            records.append(json.loads(line))
                except json.JSONDecodeError:
                    # If not NDJSON, try as single JSON
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            records = data
                        elif isinstance(data, dict):
                            records = [data]
                        else:
                            print(f"Invalid JSON format in {item['name']} - skipping.")
                            continue
                    except json.JSONDecodeError:
                        print(f"Invalid JSON in {item['name']} - skipping.")
                        continue

                if not records:
                    print(f"No records found in JSON {item['name']} - skipping.")
                    continue

                # Define schema based on your sample employee data
                schema = [
                    bigquery.SchemaField("employee_id", "INTEGER"),
                    bigquery.SchemaField("first_name", "STRING"),
                    bigquery.SchemaField("last_name", "STRING"),
                    bigquery.SchemaField("department", "STRING"),
                    bigquery.SchemaField("role", "STRING"),
                    bigquery.SchemaField("email", "STRING"),
                    bigquery.SchemaField("salary", "INTEGER"),
                    bigquery.SchemaField("hire_date", "DATE"),
                    bigquery.SchemaField("is_active", "BOOLEAN"),
                    bigquery.SchemaField("country", "STRING")
                ]

                table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{JSON_TABLE}"

                # Create table if not exists
                try:
                    bq_client.get_table(table_id)
                    print(f"Table {JSON_TABLE} already exists.")
                except NotFound:
                    table = bigquery.Table(table_id, schema=schema)
                    table = bq_client.create_table(table)
                    print(f"Created new table {JSON_TABLE}.")

                # Insert records
                errors = bq_client.insert_rows_json(table_id, records)
                if errors:
                    raise ValueError(f"Errors inserting JSON rows: {errors}")
                print(f"✓ Inserted {len(records)} records from {item['name']} into {JSON_TABLE}")

if csv_count + json_count == 0:
    print("\nNo CSV or JSON files found.")
else:
    print(f"\nSync complete! Loaded {csv_count} CSV(s) and {json_count} JSON(s) to BigQuery.")