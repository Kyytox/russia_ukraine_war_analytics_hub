from prefect import flow

# Flows
from core.process_data_warehouse.ingest.ingest_ru_block_sites import (
    flow_ingest_russia_blocked_sites,
)


@flow(
    name="DWH Flow Russia Blocked Sites",
    flow_run_name="dwh-flow-russia-blocked-sites",
    log_prints=True,
)
def flow_dwh_ru_block_sites():
    """
    Flow DWH Russia Blocked Sites
    """

    # Ingest
    flow_ingest_russia_blocked_sites()

    # Datamarts
