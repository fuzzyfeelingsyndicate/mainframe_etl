import os
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

df = pd.DataFrame(
    {
        "num_legs": [2, 4, 8, 0],
        "num_wings": [2, 0, 0, 0],
        "num_specimen_seen": [10, 2, 1, 8],
    },
    index=["falcon", "dog", "spider", "fish"],
)

scope = ["https://www.googleapis.com/auth/drive"]
creds_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

creds = Credentials.from_service_account_file(creds_path, scopes=scope)
drive = build("drive", "v3", credentials=creds)

filename = "sample.parquet"
df.to_parquet(filename)

file_metadata = {"name": filename, "parents": ["1zCu1_bKCq8rxxyuiIa3A4gfG7PbsKeqO"]}
media = MediaFileUpload(filename, mimetype="application/octet-stream")
drive.files().create(body=file_metadata, media_body=media).execute()

print("uploaded file")
