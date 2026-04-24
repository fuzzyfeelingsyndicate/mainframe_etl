import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
load_dotenv()
import io
import json
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from supabase import create_client, Client
import requests
import store_details_lines

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)


def get_drive_service():
    """Authenticate using OAuth refresh token stored as env var."""
    token_json = os.getenv("GOOGLE_OAUTH_TOKEN")
    if not token_json:
        raise RuntimeError("GOOGLE_OAUTH_TOKEN env var is not set")
    creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def upload_df_to_drive(df, file_name, folder_id):
    """Upload a DataFrame as a parquet file directly to Google Drive."""
    service = get_drive_service()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaIoBaseUpload(buffer, mimetype="application/octet-stream")
    uploaded = service.files().create(
        body=metadata, media_body=media, fields="id"
    ).execute()
    print(f"Uploaded {file_name} with ID: {uploaded.get('id')}")


def get_event_details(event_id):
    url = os.getenv("RAPID_URL")
    querystring = {"event_id":event_id}
    headers = {
        "x-rapidapi-key": os.getenv("RAPID_API_KEY"),
        "x-rapidapi-host": os.getenv("RAPID_API_HOST"),
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    return store_details_lines.extract_period0_history(response.json())


def get_data():
    response = supabase.table('events').select('event_id', 'starts').execute().data 
    events = []
    pulled_at = datetime.now(timezone.utc).date()

    timenow = datetime.now(timezone.utc)
    for event in response:
        starts = datetime.fromisoformat(event['starts'])
        if starts.tzinfo is None:
            starts = starts.replace(tzinfo=timezone.utc)
        timedif = starts - timenow
        if timedelta(0) <= timedif <= timedelta(hours=48):
            events.append(event['event_id'])
    if not FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID env var is not set")
    for event_id in events:
        try:
            df = get_event_details(event_id)
        except requests.exceptions.HTTPError as e:
            print(f"Skipping event {event_id}: {e}")
            continue
        filename = f'{event_id}{pulled_at}.parquet'
        upload_df_to_drive(df, filename, FOLDER_ID)


if __name__ == "__main__":
    get_data()

