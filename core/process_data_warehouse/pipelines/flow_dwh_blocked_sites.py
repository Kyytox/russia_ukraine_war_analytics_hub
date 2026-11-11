from prefect import flow

# Flows
from core.process_data_warehouse.ingestion.ingest_blocked_sites import (
    subflow_ingest_blocked_sites,
)

from core.process_data_warehouse.transformations.transform_blocked_sites import (
    subflow_transform_blocked_sites,
)

from core.process_data_warehouse.datamarts.dmt_blocked_sites import (
    subflow_dmt_blocked_sites,
)


@flow(
    name="DWH Flow Blocked Sites",
    flow_run_name="dwh-flow-blocked-sites",
    log_prints=True,
)
def flow_dwh_block_sites():
    """
    Flow DWH Blocked Sites in Russia
    """

    # Ingest
    # subflow_ingest_blocked_sites()
    #
    # # Transform
    # subflow_transform_blocked_sites()

    # Datamarts
    subflow_dmt_blocked_sites()


flow_dwh_block_sites()
