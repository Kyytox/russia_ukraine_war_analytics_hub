from prefect import flow

from core.process_datalake.telegram.telegram_extract import flow_telegram_extract
from core.process_datalake.telegram.telegram_cleaning import flow_telegram_cleaning
from core.process_datalake.telegram.telegram_transform import flow_telegram_transform


@flow(
    name="DLK Flow Telegram",
    flow_run_name="dlk-flow-telegram",
    log_prints=True,
)
def flow_dlk_telegram():
    """
    Flow Datalake Telegram
    """

    # Ingest
    flow_telegram_extract()

    # Clean
    flow_telegram_cleaning()

    # Transform
    flow_telegram_transform()
