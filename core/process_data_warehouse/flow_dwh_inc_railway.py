from prefect import flow

# Flows
from core.process_data_warehouse.ingest.ingest_inc_railway import (
    flow_ingest_incident_railway,
)

from core.process_data_warehouse.datamarts.datamarts_inc_railway import (
    flow_dmt_incident_railway,
)


@flow(
    name="DWH Flow Incidents Railway",
    flow_run_name="dwh-flow-incidents-railway",
    log_prints=True,
)
def flow_dwh_inc_railway():
    """
    Flow DWH Incidents Railway
    """

    # Ingest
    # flow_ingest_incident_railway()

    # Datamarts
    flow_dmt_incident_railway()
