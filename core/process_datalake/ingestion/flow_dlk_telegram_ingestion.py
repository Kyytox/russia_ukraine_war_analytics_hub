from prefect import flow
from prefect.states import Cancelling, Completed


from core.process_datalake.ingestion.telegram.telegram_extract import (
    flow_telegram_extract,
)
from core.process_datalake.ingestion.telegram.telegram_clean import (
    flow_telegram_cleaning,
)
from core.libs.utils import should_skip_late_run


@flow(
    name="DLK Flow Telegram Ingestion",
    flow_run_name="dlk-flow-telegram-ingestion",
    log_prints=True,
)
def flow_dlk_telegram_ingestion():
    """
    Flow Datalake Telegram
    """

    flow_state = Completed(
        message="Flow Datalake Telegram Ingestion completed successfully"
    )

    if should_skip_late_run(max_age_minutes=1):
        flow_state = Cancelling(message="Skipping late run")
        return flow_state

    # Ingest
    flow_telegram_extract()

    # Clean
    flow_telegram_cleaning()

    return flow_state
