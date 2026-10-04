from prefect import flow

from core.process_datalake.qualification.dlk_fusion import subflow_datalake_fusion
from core.process_datalake.qualification.dlk_theming import subflow_datalake_theming
from core.process_datalake.qualification.dlk_qualif import subflow_datalake_qualif


@flow(
    name="DLK Flow Qualification",
    flow_run_name="dlk-flow-qualification",
    log_prints=True,
)
def flow_dlk_qualification():
    """
    Flow Datalake Qualification
    """

    # Fusion
    # subflow_datalake_fusion()

    # Theming
    # subflow_datalake_theming()

    # Qualification
    subflow_datalake_qualif()
