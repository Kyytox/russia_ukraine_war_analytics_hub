from prefect import flow

# Flows
from core.process_data_warehouse.ingestion.ingest_components_weapons import (
    subflow_ingest_components_weapons,
)
from core.process_data_warehouse.transformations.transform_components_weapons import (
    subflow_transform_components_weapons,
)
from core.process_data_warehouse.datamarts.dmt_components_weapons import (
    subflow_dmt_components_weapons,
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
    # subflow_ingest_components_weapons()

    # Transform
    # subflow_transform_components_weapons()

    # Datamarts
    subflow_dmt_components_weapons()


flow_dwh_compo_weapons()
