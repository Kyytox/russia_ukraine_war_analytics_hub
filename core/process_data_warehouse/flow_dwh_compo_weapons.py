from prefect import flow

# Flows
from core.process_data_warehouse.ingest.ingest_compo_weapons import (
    flow_ingest_compo_weapons,
)
from core.process_data_warehouse.datamarts.datamarts_compo_weapons import (
    flow_dmt_compo_weapons,
)


@flow(
    name="DWH Flow Components in Weapons",
    flow_run_name="dwh-flow-components-weapons",
    log_prints=True,
)
def flow_dwh_compo_weapons():
    """
    Flow DWH Components in Weapons
    """

    # Ingest
    flow_ingest_compo_weapons()

    # Datamarts
    # flow_dmt_compo_weapons()
