import os
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from google.oauth2.service_account import Credentials

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

gauth = GoogleAuth()
gauth.credentials = Credentials.from_service_account_file(creds_path, scopes=scope)

drive = GoogleDrive(gauth)
filename = "sample.parquet"
df.to_parquet(filename)

file_drive = drive.CreateFile({"title": filename})
file_drive.SetContentFile(filename)
file_drive.Upload()

print("uploaded file")
