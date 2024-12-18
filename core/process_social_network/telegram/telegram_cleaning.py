# Process Cleaning
# - get list of accounts
# - loop over accounts
# - read raw data
# - read data clean
# - keep data not in clean data
# - format date according to utc
# - clean data
# - save data

import datetime
import pandas as pd
from prefect import flow, task

# Variables
from core.utils.variables import (
    path_telegram_raw,
    path_telegram_clean,
    dict_utc,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    get_telegram_accounts,
    concat_old_new_df,
    format_clean_text,
)


@task(name="Format date")
def format_date(df, account):
    """
    Format date according to utc

    Args:
        df: dataframe
        account: account name

    Returns:
        Dataframe with formatted date
    """

    # get utc for account
    utc = dict_utc[account]

    # add utc to date
    df["date"] = df["date"] + datetime.timedelta(hours=utc)

    # format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S").dt.tz_localize(
        None
    )

    return df


@task(name="Clean text original")
def clean_text_original(df):
    """
    Clean data

    Args:
        df: dataframe

    Returns:
        Dataframe with cleaned data
    """

    # keep only text_original not None
    df = df.dropna(subset=["text_original"])

    # format text
    df.loc[:, "text_original"] = df["text_original"].apply(format_clean_text)

    # remove text None
    df = df.dropna(subset=["text_original"])

    return df


@task(
    name="Process cleaning",
    task_run_name="Process cleaning for {account}",
)
def process_clean(account):
    """
    Process cleaning

    Args:
        account: account name

    Returns:
        None
    """
    # read raw data
    df_raw = read_data(path_telegram_raw, account)

    # read clean data
    df_clean = read_data(path_telegram_clean, account)

    # keep data not in clean data
    df = keep_data_to_process(df_raw, df_clean)
    print(f"Data to Clean: {df.shape[0]}")

    # format date
    df = format_date(df, account)

    # clean data
    df = clean_text_original(df)

    # concat data
    df = concat_old_new_df(df_raw=df_clean, df_new=df, cols=["id_message"])

    # save data
    save_data(path_telegram_clean, account, df)


@flow(name="Flow Telegram Cleaning", log_prints=True)
def job_telegram_cleaning():
    """
    Clean data from telegram
    """

    # get list of accounts
    list_accounts = get_telegram_accounts(path_telegram_raw)

    for account in list_accounts:
        print("########################################")
        print(f"Cleaning {account}")
        process_clean(account)
