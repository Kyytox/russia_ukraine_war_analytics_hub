# Process
# - Connect to Google Sheet API
# - Get data from Google Sheet
# - Check data
# - Save data

from prefect import flow

# Variables
from core.utils.variables import path_dw_sources

# Functions
from core.libs.utils import save_data
from core.libs.google_api import (
    connect_google_sheet_api,
)
from core.process_applicatifs.extract.extract_incidents_railway import (
    get_source_incident_railway,
)


@flow(name="EXTRACT SOURCES", log_prints=True)
def job_extract_sources():

    # connect to google sheet
    service = connect_google_sheet_api()

    # get source incident railway
    get_source_incident_railway(service)
