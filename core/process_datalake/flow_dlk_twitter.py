from prefect import flow


from core.process_datalake.twitter.twitter_extract import flow_twitter_extract
from core.process_datalake.twitter.twitter_cleaning import flow_twitter_cleaning


@flow(
    name="DLK Flow Twitter",
    flow_run_name="dlk-flow-twitter",
    log_prints=True,
)
def flow_dlk_twitter():
    """
    Flow Datalake Telegram
    """

    # Ingest
    flow_twitter_extract()

    # Clean
    flow_twitter_cleaning()
