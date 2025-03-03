import os.path
import pandas as pd

from prefect import task

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Variables
from core.config.paths import (
    PATH_CREDS_GOOGLE_SHEET,
    PATH_TOKEN_GOOGLE_SHEET,
)


@task(name="Connect Google Sheet API")
def connect_google_sheet_api():
    """
    Connect to Google Sheet API

    Returns:
        service: Google Sheet service
    """

    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = None

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(PATH_TOKEN_GOOGLE_SHEET):
        creds = Credentials.from_authorized_user_file(PATH_TOKEN_GOOGLE_SHEET, SCOPES)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        print("No valid credentials")
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                PATH_CREDS_GOOGLE_SHEET, SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(PATH_TOKEN_GOOGLE_SHEET, "w") as token:
            token.write(creds.to_json())

    # connect to google sheet
    service = build("sheets", "v4", credentials=creds)

    return service


@task(name="Get sheet data")
def get_sheet_data(service, spreadsheet_id, range_name):
    """
    Get data from Google Sheet

    Args:
        service: Google Sheet service
        spreadsheet_id: Google Sheet ID
        range_name: Range name

    Returns:
        Dataframe with Google Sheet data
    """

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        values = result.get("values", [])
    except HttpError as e:
        print(f"An error occurred: {e}")
        values = []

    # create dataframe
    df = pd.DataFrame(values[1:], columns=values[0])

    return df


@task(name="Update sheet data")
def update_sheet_data(service, spreadsheet_id, range_name, df):
    """
    Update data to Google Sheet

    Args:
        service: Google Sheet service
        spreadsheet_id: Google Sheet ID
        range_name: Range name
        df: Dataframe to update

    Returns:
        None
    """

    if "date" in df.columns:
        df["date"] = df["date"].astype(str)

    # get cols and values
    cols = df.columns.tolist()
    values = df.values.tolist()

    # clear sheet
    result = (
        service.spreadsheets()
        .values()
        .clear(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    print(f"{result.get('clearedRanges')} ranges cleared.")

    # update values
    body = {"values": [cols] + values}
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )

    print(f"{result.get('updatedCells')} cells updated.")
