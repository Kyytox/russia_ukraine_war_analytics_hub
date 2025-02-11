import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.states import Completed, Failed

# Variables
from core.config.paths import PATH_DWH_SOURCES
from core.config.variables import ID_EXCEL_RUSSIA_BLOCK_SITE
from core.config.dwh_corresp_schema import CORRESP_SCHEMA_RUSSIA_BLOCK_SITES

# Functions
from core.libs.utils import (
    save_data,
    create_artifact,
    upd_data_artifact,
    rename_cols,
)
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


@task(name="Format values", task_run_name="format-values")
def format_values(df):
    """
    Format the values in the columns

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with formatted values
    """

    # replcae '' and '-' by np.nan
    df["subcategory"] = df["subcategory"].replace(["", "-"], np.nan)

    # convert date
    df["date_blocked"] = pd.to_datetime(df["date_blocked"], errors="coerce")

    return df


@flow(
    name="DWH Subflow Ingestion Russia Blocked Sites",
    flow_run_name="dwh-subflow-ingestion-russia-blocked-sites",
    log_prints=True,
)
def flow_ingest_russia_blocked_sites():
    """
    Integration of Russia blocked sites in the Data Warehouse
    """

    # vars google
    spreadsheet_id = ID_EXCEL_RUSSIA_BLOCK_SITE
    range_name = "Blocked Domains!A7:F"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get data from google sheet
    df = get_sheet_data(service, spreadsheet_id, range_name)

    # rename cols
    df = rename_cols(df, CORRESP_SCHEMA_RUSSIA_BLOCK_SITES)

    # Format values
    df = format_values(df)

    upd_data_artifact(f"Ingestion Russia blocked sites", df.shape[0])

    # save data
    save_data(PATH_DWH_SOURCES, "russia_blocked_sites", df)

    # create artifact
    create_artifact("dwh-subflow-ingestion-russia-blocked-sites-artifact")

    return Completed(message="Ingestion Russia Blocked Sites completed")
