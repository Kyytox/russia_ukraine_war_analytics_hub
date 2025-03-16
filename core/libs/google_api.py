import pandas as pd

from prefect import task

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# Variables
from core.config.paths import (
    PATH_SERVICE_ACCOUNT,
)


@task(name="Connect Google Sheet API")
def connect_google_sheet_api():
    """
    Connect to Google Sheet API

    Returns:
        service: Google Sheet service
    """
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    credentials = service_account.Credentials.from_service_account_file(
        PATH_SERVICE_ACCOUNT, scopes=SCOPES
    )
    service = build("sheets", "v4", credentials=credentials)

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
