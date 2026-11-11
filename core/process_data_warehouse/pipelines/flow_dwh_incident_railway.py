from prefect import flow

# Flows
from core.process_data_warehouse.ingestion.ingest_incident_railway import (
    subflow_ingest_incident_railway,
)

from core.process_data_warehouse.transformations.transform_incident_railway import (
    subflow_transform_incidents_railway,
)

from core.process_data_warehouse.datamarts.dmt_incident_railway import (
    subflow_dmt_incident_railway,
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

    # Ingestion
    # subflow_ingest_incident_railway()

    # Transformations
    # subflow_transform_incidents_railway()

    # Datamarts
    subflow_dmt_incident_railway()


flow_dwh_inc_railway()
