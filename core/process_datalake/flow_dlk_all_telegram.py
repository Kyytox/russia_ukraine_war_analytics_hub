from prefect import flow


from core.process_datalake.ingestion.flow_dlk_telegram_ingestion import (
    flow_dlk_telegram_ingestion,
)

from core.process_datalake.transform.flow_dlk_telegram_transform import (
    flow_dlk_telegram_transform,
)
from core.process_datalake.qualification.flow_dlk_qualification import (
    flow_dlk_qualification,
)


@flow(
    name="DLK Flow ALL Telegram",
    flow_run_name="dlk-flow-all-telegram",
    log_prints=True,
)
def flow_dlk_all_telegram():
    """
    Flow Datalake Telegram
    """

    # flow_dlk_telegram_ingestion()
    flow_dlk_telegram_transform()
    flow_dlk_qualification()
