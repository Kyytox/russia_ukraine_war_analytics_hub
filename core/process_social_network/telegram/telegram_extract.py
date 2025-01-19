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
from prefect import flow, task
from prefect.cache_policies import NONE

# Variables
from core.config.paths import PATH_TELEGRAM_RAW
from core.config.variables import LIST_ACCOUNTS_TELEGRAM

# Functions
from core.libs.telegram_api import telegram_connect
from core.libs.utils import (
    read_data,
    save_data,
    concat_old_new_df,
)


@task(
    name="Get Telegram Messages",
    task_run_name="get-messages-{account}",
    cache_policy=NONE,
)
async def get_messages(client, account, df_raw):
    """
    Get messages from Telegram

    Args:
        client: TelegramClient
        account: account name
        df_raw: raw dataframe

    Returns:
        df: dataframe with messages
    """
    # init variables
    data = []

    # get last date or id
    last_date_message = (
        datetime.datetime(2021, 12, 31, 22, 59, 59) if df_raw.empty else None
    )
    last_id_message = 0 if df_raw.empty else df_raw["id_message"].max()

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
                    "ID": f"{account}_{message.id}",
                    "account": account,
                    "id_message": message.id,
                    "date": message.date,
                    "text_original": message.message,
                }
            )

    # create df
    df = pd.DataFrame(data)
    print(f"Extracted {df.shape[0]} messages")

    return df


@flow(
    name="Flow Single Telegram Extract",
    flow_run_name="Flow-telegram-extract-{account}",
    log_prints=True,
)
def process_extract(client, account):
    """
    Process extract
    """

    # get raw data
    df_raw = read_data(PATH_TELEGRAM_RAW, account)

    # get messages
    with client:
        df = client.loop.run_until_complete(get_messages(client, account, df_raw))

    # concat data
    df = concat_old_new_df(df_raw, df, cols=["ID"])

    # save data
    save_data(PATH_TELEGRAM_RAW, account, df=df)


@flow(
    name="Flow Master Telegram Extract",
    flow_run_name="Flow-master-telegram-extract",
    description="Extract Telegram messages from accounts",
    log_prints=True,
)
def job_telegram_extract():
    """
    Extract messages from Telegram
    """
    print("********************************")
    print("Start extracting Telegram")
    print("********************************")

    # Connect to Telegram
    client = telegram_connect()

    # get list of accounts
    # list_accounts = get_telegram_accounts(PATH_TELEGRAM_RAW)
    list_accounts = LIST_ACCOUNTS_TELEGRAM

    # extract messages for each account
    for account in list_accounts:
        print(f"Extracting {account}")
        process_extract(client, account)
