# Process
# - get filter data
# - get data to classify (Excel from Google Drive)
# - merge data


import os.path
import pandas as pd

from prefect import flow, task

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Functions
from code.libs.utils import read_data
from code.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from code.utils.variables import (
    path_telegram_filter,
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@task(name="Clean columns")
def clean_columns(df):
    """
    Clean columns
    - adapt columns names for news data
    - adapt columns names for filter data
    - rename columns
    - drop cols _x and _y

    Args:
        df: dataframe with all data merged

    Returns:
        df: dataframe with clean columns

    """
    # extract data date_x is null
    df_x = df[df["date_x"].isnull()]
    df = df.dropna(subset=["date_x"])

    # drop _x cols and rename _y cols
    df_x = df_x.drop(columns=[col for col in df_x.columns if col.endswith("_x")])
    df_x = df_x.rename(columns={col: col.replace("_y", "") for col in df_x.columns})

    # extract data date_y is null
    df_y = df[df["date_y"].isnull()]
    df = df.dropna(subset=["date_y"])

    # drop _y cols and rename _x cols
    df_y = df_y.drop(columns=[col for col in df_y.columns if col.endswith("_y")])
    df_y = df_y.rename(columns={col: col.replace("_x", "") for col in df_y.columns})

    df = df.rename(
        columns={
            "date_x": "date",
            "text_original_x": "text_original",
            "text_translate_x": "text_translate",
            "is_add_to_final_dataset_y": "is_add_to_final_dataset",
            "is_incident_railway_y": "is_incident_railway",
            "url_x": "url",
        }
    )

    # drop cols _x and _y
    df = df.drop(
        columns=[col for col in df.columns if col.endswith("_x") or col.endswith("_y")]
    )

    # concat
    df = pd.concat([df, df_x, df_y], ignore_index=True)

    # remove \n in text original
    df["text_original"] = df["text_original"].str.replace("\n", " ")

    print(f"Messages to classify: {df.shape}")

    # reorder
    df = df[
        [
            "account",
            "id_message",
            "date",
            "text_original",
            "text_translate",
            "is_add_to_final_dataset",
            "is_incident_railway",
            "url",
        ]
    ].sort_values("date")

    return df


@task(name="Merge filter to news")
def merge_filter_to_news(df_filter, df_to_classify):
    """
    Merge data
    Add new data potential news about railways

    Args:
        df_filter: dataframe with filter data
        df_to_classify: dataframe with news data

    Returns:
        Dataframe with merge data
    """
    print(f"Filter data: {df_filter.shape}")
    print(f"Classify data: {df_to_classify.shape}")

    print(
        f"Data integrated to data final: {df_to_classify[df_to_classify['is_add_to_final_dataset']].shape}"
    )

    print(
        f"Data classified not incident: {df_to_classify[~df_to_classify['is_incident_railway']].shape}"
    )

    # merge data
    df = pd.merge(
        df_filter,
        df_to_classify,
        how="outer",
        on=["account", "id_message"],
    )
    print(f"Data merged: {df.shape}")

    return df


@task(name="Convert columns")
def convert_cols(df):
    """
    Convert columns
    """
    # convert str to bool
    df["is_add_to_final_dataset"] = df["is_add_to_final_dataset"] == "TRUE"
    df["is_incident_railway"] = df["is_incident_railway"] == "TRUE"

    # convert date
    df["date"] = pd.to_datetime(df["date"])

    # convert id_message to int
    df["id_message"] = df["id_message"].astype(int)

    return df


@flow(name="Filter to classify", log_prints=True)
def job_filter_to_classify():
    """
    Filter data to classify
    """

    spreadsheet_id = "1G6C-EG9r3m7nm8jl_zCiHszj1S4W0uvMyTdtrHaePfY"
    range_name = "news_railway_to_classified"
    # spreadsheet_id = "1_UUuTh8YyoWqStULX9qQNZcnxXnBZ013Ojjpbg2CTaw"
    # range_name = "Sheet1"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get telegrame filter data
    df_filter = read_data(path_telegram_filter, "incidents_railway")

    # get data from Google Sheet
    df_to_classify = get_sheet_data(service, spreadsheet_id, range_name)

    # convert columns
    df_to_classify = convert_cols(df_to_classify)

    # merge data
    df = merge_filter_to_news(df_filter, df_to_classify)

    # clean columns
    df = clean_columns(df)

    # save data
    update_sheet_data(service, spreadsheet_id, range_name, df)
