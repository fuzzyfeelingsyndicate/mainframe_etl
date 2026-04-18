import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def get_drive_service():
    creds_json = os.getenv("GOOGLE_SA_KEY")
    creds_info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def upload_parquet(df, file_name, folder_id):
    service = get_drive_service()
    buffer = io.BytesIO()
    file_name = os.path.basename(file_path)
    metadata = {"name": file_name, "parents": [folder_id]}
    media = MediaFileUpload(file_path, mimetype="application/octet-stream")
    uploaded = service.files().create(
        body=metadata, media_body=media, fields="id"
    ).execute()
    print(f"Uploaded {file_name} with ID: {uploaded.get('id')}")

data = {
    "eevent id " : [101,102],
    "home_tema" : ['arsenal', 'chelsea']
}

df = pd.dataframe(data)

upload_parquet( , '1keVxmV4jfm0esecJA0LCYmbQohNWBf0F')
