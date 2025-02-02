# Process
# - For each filter
# - Get data classified
# - Get data Excel in Google Drive
# - Check if data is to update
# - Update data in Google Drive with data Classified

import pandas as pd

from prefect import flow, task

# Functions
from core.libs.utils import read_data
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from core.config.paths import (
    PATH_CLASSIFY_DATALAKE,
)
from core.config.variables import (
    ID_EXCEL_INCIDENT_RAILWAY,
    ID_EXCEL_INCIDENT_ARREST,
)


@task(name="Process Incidents Arrest")
def process_incidents_arrest(service):
    """
    Process Incidents Arrest
    """

    dict_cols = {
        "ID": "ID",
        "IDX": "IDX",
        "class_region": "Region",
        "class_location": "Location",
        "class_arrest_date": "Arrest Date",
        "class_arrest_reason": "Arrest Reason",
        "class_app_laws": "Applied Laws",
        "class_sentence_years": "Sentence Years",
        "class_sentence_date": "Sentence Date",
        "class_person_name": "Person Name",
        "class_person_age": "Person Age",
        "url": "Sources",
        "class_comments": "Comments",
    }

    # get data Excel in Google Drive
    spreadsheet_id = ID_EXCEL_INCIDENT_ARREST
    range_name = "Arrest In Russia - DATA"

    # get data in Google Drive
    df_excel = get_sheet_data(service, spreadsheet_id, range_name)

    # get data classified
    df = read_data(PATH_CLASSIFY_DATALAKE, "classify_inc_arrest")

    # rename columns
    df.rename(columns=dict_cols, inplace=True)

    # put in order
    df = df[list(dict_cols.values())]

    # remove ID
    df.drop(columns=["ID"], inplace=True)

    # convert to date
    df_excel["Arrest Date"] = pd.to_datetime(df_excel["Arrest Date"], errors="coerce")
    df_excel["Sentence Date"] = pd.to_datetime(
        df_excel["Sentence Date"], errors="coerce"
    )

    print(f"Data classified: {df}")
    print(f"Data classified: {df.dtypes}")
    print(f"Data in Google Drive: {df_excel}")
    print(f"Data in Google Drive: {df_excel.dtypes}")

    # check if data is to update
    if df.shape[0] > df_excel.shape[0]:
        # Add new data to Excel
        df = pd.concat([df_excel, df]).drop_duplicates().reset_index(drop=True)

    print(f"Data to update in Google Drive: {df}")

    # convert dat to string
    df["Arrest Date"] = df["Arrest Date"].dt.strftime("%m/%d/%Y")
    df["Sentence Date"] = df["Sentence Date"].dt.strftime("%m/%d/%Y")

    # fillna
    df = df.fillna("")

    print(f"Data to update in Google Drive: {df}")

    # save data
    update_sheet_data(service, spreadsheet_id, range_name, df)


@flow(name="Classify to Excel Final", log_prints=True)
def flow_classify_to_excel_final():
    """
    Update data in Google Drive with data Classified
    """

    # connect to google sheet
    service = connect_google_sheet_api()

    # Process Incidents Arrest
    process_incidents_arrest(service)
