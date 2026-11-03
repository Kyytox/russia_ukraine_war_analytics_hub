from prefect import flow
from prefect.states import Completed

# Variables
from core.common.parquet import save_parquet
from core.config.variables import ID_EXCEL_RUSSIA_BLOCK_SITE
from core.config.paths import PATH_DWH_BRONZE_BLOCKED_SITES
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


@flow(
    name="DWH Subflow Ingestion locked Sites",
    flow_run_name="dwh-subflow-ingestion-blocked-sites",
    log_prints=True,
)
def subflow_ingest_blocked_sites():
    """
    Integration of blocked sites in RUssia in the Data Warehouse
    """

    # vars google
    spreadsheet_id = ID_EXCEL_RUSSIA_BLOCK_SITE
    range_name = "Blocked Domains!A7:F"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get data from google sheet
    df = get_sheet_data(service, spreadsheet_id, range_name)

    # save data
    save_parquet(PATH_DWH_BRONZE_BLOCKED_SITES, "blocked_sites", df)

    return Completed(message="Ingestion Russia Blocked Sites completed")
