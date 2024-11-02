import yaml

from telethon import TelegramClient

# Variables
from utils.variables import credentials_path


def telegram_connect():
    """
    Connect to Telegram

    Returns:
        - client: TelegramClient
    """
    # get credentials
    with open(credentials_path) as file:
        credentials = yaml.safe_load(file)

    # Connect to Telegram
    api_id = credentials["telegram"]["api_id"]
    api_hash = credentials["telegram"]["api_hash"]
    client = TelegramClient("kyytox", api_id, api_hash)

    return client
