# Process
# - get csv in Google Drive
# - get classify data
# - format data for update in Google Drive
# - Save it for archive
# - update data classified to Google Drive
#

import pandas as pd
from datetime import datetime

from prefect import flow, task

# Functions
from core.libs.utils import read_data, retype_cols, upd_data_artifact, create_artifact
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from core.config.paths import PATH_CLASSIFY_DATALAKE

from core.config.variables import ID_EXCEL_INCIDENT_ARREST
from core.config.schemas import SCHEMA_EXCEL_ARREST


dict_corr_schema = {
    # "ID": "ID",
    "IDX": "IDX",
    "class_region": "Region",
    "class_location": "Location",
    "class_arrest_date": "Arrest Date",
    "class_arrest_reason": "Arrest Reason",
    "class_app_laws": "Applicable Laws",
    "class_media_post": "Media Post",
    "class_sentence_time": "Sentence Time",
    "class_sentence_date": "Sentence Date",
    "class_location_detention": "Location Detention",
    "class_person_name": "Person Name",
    "class_person_age": "Person Age",
    "class_person_job": "Person Job",
    "class_sources": "Source Links",
    "class_comments": "Comments",
}


@task(name="Format Columns", task_run_name="format-columns")
def format_columns(df):
    """
    Format Columns

    Args:
        df: dataframe classify

    Returns:
        Dataframe with formatted columns
    """

    # update columns name
    df = df.rename(columns=dict_corr_schema)

    # sort by date
    df = df.sort_values(by="Arrest Date", ascending=True)

    # convert date to str
    df["Arrest Date"] = df["Arrest Date"].dt.strftime("%m/%d/%Y")
    df["Sentence Date"] = df["Sentence Date"].dt.strftime("%m/%d/%Y")

    # convert Int to str
    df["Person Age"] = df["Person Age"].astype(str)

    # replace values
    df = df.replace("None", None)
    df = df.where(pd.notnull(df), None)

    # remove \n
    df["Source Links"] = df["Source Links"].apply(lambda x: x.replace("\n", ""))

    # add jump line in source links, after each ,
    df["Source Links"] = df["Source Links"].apply(lambda x: x.replace(",", ",\n"))

    # reorder cols
    df = df[dict_corr_schema.values()]

    return df


@flow(
    name="Flow Master Classify to Cloud",
    flow_run_name="flow-master-classify-to-cloud",
    log_prints=True,
)
def flow_arrest_classify_to_cloud():
    """
    Arrestation Classify to Cloud
    """

    spreadsheet_id = ID_EXCEL_INCIDENT_ARREST
    range_name = "Arrest In Russia - DATA"
    today = datetime.today().strftime("%Y_%m_%d_%H_%M_%S")

    # connect to google sheet
    service = connect_google_sheet_api()

    """
    Init Data
    """
    # get data from Google Sheet
    df_excel_final = get_sheet_data(service, spreadsheet_id, range_name)
    print(df_excel_final)

    # get classify data
    df_classify = read_data(PATH_CLASSIFY_DATALAKE, "classify_inc_arrest")
    print(df_classify.info())

    """
    Process Data
    """
    # Compare data
    if df_excel_final.shape[0] > df_classify.shape[0]:
        print("WARNING: Excel Final has more data than Classify !")
        return

    # retype columns
    df_classify = retype_cols(df_classify, SCHEMA_EXCEL_ARREST)

    # format data
    df_classify = format_columns(df_classify)
    print(df_classify)
    """
    Update Data
    """

    # save Excel Final (archive)
    df_excel_final.to_csv(f"{PATH_CLASSIFY_DATALAKE}/old_excel_arrest_{today}.csv")

    try:
        update_sheet_data(service, spreadsheet_id, range_name, df_classify)
    except Exception as e:
        print("Retry Update", e)
        # connect to google sheet
        service = connect_google_sheet_api()
        update_sheet_data(service, spreadsheet_id, range_name, df_classify)

    # create artifact
    print(f"Rows updated in Google Sheet: {df_classify.shape[0]}")
    upd_data_artifact(f"Rows updated in Google Sheet", df_classify.shape[0])
    create_artifact("flow-arrest-classify-to-cloud-artifact")
