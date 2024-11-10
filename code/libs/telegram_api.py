import yaml

from telethon import TelegramClient
from prefect import flow, task

# Variables
from code.utils.variables import path_creds_telegram


@task(name="Telegram connect")
def telegram_connect():
    """
    Connect to Telegram

    Returns:
        - client: TelegramClient
    """
    # get credentials
    with open(path_creds_telegram) as file:
        credentials = yaml.safe_load(file)

    # Connect to Telegram
    api_id = credentials["telegram"]["api_id"]
    api_hash = credentials["telegram"]["api_hash"]
    client = TelegramClient("kyytox", api_id, api_hash)

    return client
