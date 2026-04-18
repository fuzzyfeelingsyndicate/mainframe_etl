import os
import io
import json
import pandas as pd
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")


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


if __name__ == "__main__":
    # Example: replace this with your actual DataFrame
    data = {
        "event_id": [101, 102],
        "home_team": ["Arsenal", "Chelsea"],
        "away_team": ["Liverpool", "Man City"],
        "home_odds": [1.85, 2.10],
        "away_odds": [3.40, 3.25],
    }
    df = pd.DataFrame(data)

    if not FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID env var is not set")

    upload_df_to_drive(df, "events.parquet", FOLDER_ID)
