from prefect import flow

# Flows
from core.process_data_warehouse.ingest.ingest_ru_officers_kiu import (
    flow_ingest_ru_officers_kiu,
)
from core.process_data_warehouse.datamarts.datamarts_ru_officers_kiu import (
    flow_dmt_ru_officers_kiu,
)


@flow(
    name="DWH Flow Russian Officers KIU",
    flow_run_name="dwh-flow-ru-officers-kiu",
    log_prints=True,
)
def flow_dwh_ru_officers_kiu():
    """
    Flow DWH Russian Officers KIU
    """

    # Ingest
    flow_ingest_ru_officers_kiu()

    # Datamarts
    # flow_dmt_ru_officers_kiu()
