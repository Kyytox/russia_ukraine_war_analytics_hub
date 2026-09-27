from prefect import flow

from core.process_datalake.transform.telegram.telegram_transform import (
    flow_telegram_transform,
)


@flow(
    name="DLK Flow Telegram Transform",
    flow_run_name="dlk-flow-telegram-transform",
    log_prints=True,
)
def flow_dlk_telegram_transform():
    """
    Flow Datalake Telegram
    """

    # Transform
    flow_telegram_transform()
