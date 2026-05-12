import io
import json
import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")


def get_drive_service():
    token_json = os.getenv("GOOGLE_OAUTH_TOKEN")
    if not token_json:
        raise RuntimeError("GOOGLE_OAUTH_TOKEN env var is not set")
    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def list_parquet_files(service, folder_id):
    query = f"'{folder_id}' in parents and name contains '.parquet' and trashed=false"
    all_files = []
    page_token = None
    while True:
        results = service.files().list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        all_files.extend(results.get("files", []))
        page_token = results.get("nextPageToken")
        if not page_token:
            break
    print(f"[DEBUG] Found {len(all_files)} parquet files in folder")
    return all_files
    return results.get("files", [])


def download_file(service, file_id):
    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buffer.seek(0)
    return buffer


def upload_batch(service, folder_id, df):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    metadata = {"name": "batch.parquet", "parents": [folder_id]}
    media = MediaIoBaseUpload(buffer, mimetype="application/octet-stream")
    service.files().create(body=metadata, media_body=media, fields="id").execute()
    print(f"Uploaded batch.parquet with {len(df)} rows")


def delete_files(service, files):
    for f in files:
        service.files().delete(fileId=f["id"]).execute()
        print(f"Deleted {f['name']}")


if __name__ == "__main__":
    if not FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID env var is not set")
    print(f"[DEBUG] Using FOLDER_ID: {FOLDER_ID}")

    service = get_drive_service()
    files = list_parquet_files(service, FOLDER_ID)

    if not files:
        print("No parquet files found in folder")
    else:
        dfs = []
        for f in files:
            print(f"Reading {f['name']}")
            buf = download_file(service, f["id"])
            dfs.append(pd.read_parquet(buf))

        if not dfs:
            print("No files to process")
        else:
            batch = pd.concat(dfs, ignore_index=True).drop_duplicates().reset_index(drop=True)
            upload_batch(service, FOLDER_ID, batch)
            delete_files(service, files)
