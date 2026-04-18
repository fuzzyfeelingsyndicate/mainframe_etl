"""
Run this script ONCE on your local machine to generate the OAuth token.
Copy the printed JSON and save it as GOOGLE_OAUTH_TOKEN in GitHub Secrets.

Prerequisites:
  1. You must have a credentials.json file from Google Cloud Console
     (OAuth 2.0 Client ID -> Desktop app -> Download JSON)
  2. pip install google-auth-oauthlib
"""

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
creds = flow.run_local_server(port=0)

print("\n=== Copy everything below this line into GOOGLE_OAUTH_TOKEN secret ===\n")
print(creds.to_json())
print("\n=== Copy everything above this line ===")
