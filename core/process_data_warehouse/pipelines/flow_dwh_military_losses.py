from prefect import flow

# Flows
from core.process_data_warehouse.ingest.ingest_military_losses import (
    flow_ingest_military_losses,
)
from core.process_data_warehouse.datamarts.datamarts_military_losses import (
    flow_dmt_military_losses,
)


@flow(
    name="DWH Flow Military Losses",
    flow_run_name="dwh-flow-military-losses",
    log_prints=True,
)
def flow_dwh_military_losses():
    """
    Flow DWH Military Losses
    """

    # Ingest
    flow_ingest_military_losses()

    # Datamarts
    # flow_dmt_military_losses()
