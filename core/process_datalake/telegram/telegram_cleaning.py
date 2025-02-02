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
from core.config.paths import (
    PATH_TELEGRAM_RAW,
    PATH_TELEGRAM_CLEAN,
)
from core.config.variables import (
    DICT_UTC,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    get_telegram_accounts,
    concat_old_new_df,
    format_clean_text,
    upd_data_artifact,
    create_artifact,
)


@task(name="Update UTC Date", task_run_name="update-utc-date-{account}")
def update_utc_date(df, account):
    """
    Format date according to utc

    Args:
        df: dataframe
        account: account name

    Returns:
        Dataframe with formatted date
    """

    # get utc for account
    utc = DICT_UTC[account]

    # add utc to date
    df["date"] = df["date"] + datetime.timedelta(hours=utc)

    # format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S").dt.tz_localize(
        None
    )

    return df


@task(name="Clean text original", task_run_name="clean-text-original")
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


@flow(
    name="Flow Single Telegram Cleaning",
    flow_run_name="flow-telegram-cleaning-{account}",
    log_prints=True,
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
    df_raw = read_data(PATH_TELEGRAM_RAW, account)

    # read clean data
    df_clean = read_data(PATH_TELEGRAM_CLEAN, account)

    # keep data not in clean data
    df = keep_data_to_process(df_raw, df_clean)
    print(f"Data to Clean: {df.shape[0]}")

    # format date
    df = update_utc_date(df, account)

    # clean data
    df = clean_text_original(df)

    # update artifact
    upd_data_artifact(account, df.shape[0])

    # concat data
    df = concat_old_new_df(df_raw=df_clean, df_new=df, cols=["ID"])

    # save data
    save_data(PATH_TELEGRAM_CLEAN, account, df)


@flow(
    name="Flow Master Telegram Cleaning",
    flow_run_name="flow-master-telegram-cleaning",
    log_prints=True,
)
def flow_telegram_cleaning():
    """
    Clean data from telegram
    """

    print("********************************")
    print("Start cleaning Telegram")
    print("********************************")

    # get list of accounts
    list_accounts = get_telegram_accounts(PATH_TELEGRAM_RAW)

    for account in list_accounts:
        print(f"Cleaning {account}")
        process_clean(account)

    # create artifact
    create_artifact("flow-master-telegram-cleaning-artifact")
