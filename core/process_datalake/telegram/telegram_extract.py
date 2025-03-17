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
    upd_data_artifact,
    create_artifact,
)


@task(
    name="Get Telegram Messages",
    task_run_name="get-messages-{account}",
    cache_policy=NONE,
)
async def collect_messages(
    client: object, account: str, last_date: datetime, last_id: int
) -> pd.DataFrame:
    """
    Collect messages from account

    Args:
        client: TelegramClient
        account: account name
        last_date: last date of message
        last_id: last id of message

    Returns:
        df: dataframe with new messages
    """
    # init variables
    data = []

    async for message in client.iter_messages(
        account,
        offset_date=last_date,
        offset_id=last_id,
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

    return pd.DataFrame(data)


@task(
    name="Get last date or id",
    task_run_name="get-last-date-or-id",
)
def get_last_date_or_id(df_raw_acc: pd.DataFrame) -> tuple:
    """
    Get last date or id

    Args:
        df_raw_acc: dataframe with data already extracted from account

    Returns:
        last_date: last date
        last_id: last id
    """

    if df_raw_acc.empty:
        last_date = datetime.datetime(2021, 12, 31, 22, 59, 59)
        last_id = 0
    else:
        last_date = None
        last_id = df_raw_acc["id_message"].max()

    print("last_id:", last_id)
    print("last_date:", last_date)

    return last_date, last_id


@flow(
    name="DLK Subflow Telegram Extract",
    flow_run_name="dlk-subflow-telegram-extract-{account}",
    log_prints=True,
)
def process_extract(
    client: object, account: str, df_raw_acc: pd.DataFrame
) -> pd.DataFrame:
    """
    Process extract

    Args:
        client: TelegramClient
        account: account name
        df_raw_acc: data already extracted from account

    Returns:
        df: dataframe with old and new messages
    """
    print("Account old data shape:", df_raw_acc.shape)

    # get last date or id
    last_date, last_id = get_last_date_or_id(df_raw_acc)

    # get messages
    with client:
        df = client.loop.run_until_complete(
            collect_messages(client, account, last_date, last_id)
        )

    # update data artifact
    print(f"Messages extracted from {account}: {df.shape[0]}")
    upd_data_artifact(f"Messages extracted from {account}", df.shape[0])

    return df


@flow(
    name="DLK Flow Telegram Extract",
    flow_run_name="dlk-flow-telegram-extract",
    description="Extract Telegram messages from accounts",
    log_prints=True,
)
def flow_telegram_extract():
    """
    Extract messages from Telegram
    """
    # Connect to Telegram
    client = telegram_connect()

    # get list of accounts
    list_accounts = LIST_ACCOUNTS_TELEGRAM

    # get raw Data already extracted
    df_raw = read_data(PATH_TELEGRAM_RAW, "raw_telegram")

    # Init dfs
    df_new_data = pd.DataFrame()  # new data to extract
    df_raw_acc = pd.DataFrame()  # data already extracted for account

    # Collect message from all accounts
    for account in list_accounts:
        print(f"Extracting {account}")

        if df_raw.shape[0] > 0 and account in df_raw["account"].unique():
            df_raw_acc = df_raw[df_raw["account"] == account]
        else:
            df_raw_acc = pd.DataFrame()

        # extract messages
        df_new_data = pd.concat(
            [df_new_data, process_extract(client, account, df_raw_acc)]
        )

    # concat data
    df_final = concat_old_new_df(df_raw, df_new_data, cols=["ID"])
    print("Final shape:", df_final.shape)

    # create artifact
    create_artifact("dlk-flow-telegram-extract-artifact")

    # save data
    save_data(PATH_TELEGRAM_RAW, "raw_telegram", df_final, ["account"])
