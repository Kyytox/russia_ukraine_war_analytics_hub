# Process
# - get credentials
# - Connect to Telegram
# - get list of account to extract
# - loop over accounts
# - get raw data
# - check if account is already in data
# - if YES -> get last id
# - if NO -> retrive from date = 2021-12-31 22:59:59
# - extract messages
# - save messages

import datetime
import pandas as pd

# Variables
from utils.variables import (
    list_accounts_telegram,
    path_telegram_raw,
)

# Functions
from libs.telegram_api import telegram_connect
from libs.utils import read_data, save_data


async def get_messages(client, account, last_id_message):
    """
    Get messages from Telegram

    Args:
        client: TelegramClient
        chat: chat name
        last_id_message: last id message

    Returns:
        df: dataframe with messages
    """

    # init variables
    data = []
    last_date_message = None

    if last_id_message == 0:
        # to get message after 2021 (-1h for UTC)
        last_date_message = datetime.datetime(2021, 12, 31, 22, 59, 59)

    print("Start extracting messages from")
    print("last_id_message:", last_id_message)
    print("last_date_message:", last_date_message)

    async for message in client.iter_messages(
        account,
        offset_date=last_date_message,
        offset_id=last_id_message,
        reverse=True,
    ):
        # add to data
        if message.text != "":
            data.append(
                {
                    "account": account,
                    "id_message": message.id,
                    "date": message.date,
                    "text_original": message.message,
                }
            )

    # create df
    df = pd.DataFrame(data)

    return df


def telegram_extract():
    """
    Extract messages from Telegram
    """
    print("--------------------------------")
    print("Extracting messages from Telegram")
    print("--------------------------------")

    # init variables
    last_id_message = 0

    # Connect to Telegram
    client = telegram_connect()

    # extract messages for each account
    for account in list_accounts_telegram:
        print("------------------")
        print(account)

        # get raw data
        df_raw = read_data(path_telegram_raw, account)

        # get last id
        if not df_raw.empty:
            last_id_message = df_raw["id_message"].max()

        with client:
            df = client.loop.run_until_complete(
                get_messages(client, account, last_id_message)
            )

        # concat data
        print("Number of messages extracted:", len(df))
        df = pd.concat([df_raw, df])
        print("Number of messages after concat:", len(df))

        # drop duplicates
        df = df.drop_duplicates(subset=["id_message"]).reset_index(drop=True)

        # save data
        save_data(path_telegram_raw, account, df_raw, df)
