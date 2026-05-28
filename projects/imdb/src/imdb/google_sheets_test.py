import os.path
import pathlib

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import gspread
import polars as pl

# If modifying these scopes, delete the file token.json.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print(
            pathlib.Path(__file__).parent.parent.parent.resolve()
            / "google_api_credentials.json"
        )
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "google_api_credentials.json", SCOPES
            )

        creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        gc = gspread.authorize(creds)

        sheet = gc.open_by_key("1TD_zFb5lqa4-7hAjepTfIbUVRKDvuL_PS4WAE_Am3Ek")

        gekeken_ws = sheet.worksheet("gekeken")
        nog_kijken_ws = sheet.worksheet("nog kijken")

        df_gekeken = pl.DataFrame(gekeken_ws.get_all_records())
        df_nog_kijken = pl.DataFrame(nog_kijken_ws.get_all_records())

        print(df_gekeken)
        print()
        print(df_nog_kijken)
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
