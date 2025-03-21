import yaml

from telethon import TelegramClient
from prefect import flow, task

# Variables
from core.config.paths import PATH_CREDS_API


@task(name="Telegram Connect API", task_run_name="telegram-connect-api", tags=["API"])
def telegram_connect():
    """
    Connect to Telegram

    Returns:
        - client: TelegramClient
    """
    # get credentials
    with open(PATH_CREDS_API) as file:
        credentials = yaml.safe_load(file)

    # Connect to Telegram
    api_id = credentials["telegram"]["api_id"]
    api_hash = credentials["telegram"]["api_hash"]
    account_name = credentials["telegram"]["account_name"]
    client = TelegramClient(account_name, api_id, api_hash)

    return client
