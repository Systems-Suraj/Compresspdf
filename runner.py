# ---- Auth: OAuth Refresh Token (no service account) ----
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

creds = Credentials(
    token=None,
    refresh_token=os.environ["GOOGLE_OAUTH_REFRESH_TOKEN"],
    token_uri="https://oauth2.googleapis.com/token",
    client_id=os.environ["GOOGLE_OAUTH_CLIENT_ID"],
    client_secret=os.environ["GOOGLE_OAUTH_CLIENT_SECRET"],
    scopes=SCOPES,
)
# Fetch a fresh access token:
creds.refresh(Request())

drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)
sheets_svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
