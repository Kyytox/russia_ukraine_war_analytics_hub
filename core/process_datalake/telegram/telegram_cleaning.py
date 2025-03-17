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
    concat_old_new_df,
    format_clean_text,
    upd_data_artifact,
    create_artifact,
)


@task(name="Update UTC date", task_run_name="update-utc-date")
def update_utc_date(df):
    """
    Format date according to utc

    Args:
        df: dataframe

    Returns:
        Dataframe with formatted date
    """

    # create col UTC
    df["utc_offset"] = df["account"].map(DICT_UTC)

    # convert to timedelta
    df["utc_offset"] = pd.to_timedelta(df["utc_offset"], unit="h")

    # add utc to date
    df["date"] = df["date"] + df["utc_offset"]

    # format date
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M:%S").dt.tz_localize(
        None
    )

    # drop col utc
    df = df.drop(columns=["utc_offset"])

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
    name="DLK Flow Telegram Cleaning",
    flow_run_name="dlk-flow-telegram-clean",
    log_prints=True,
)
def flow_telegram_cleaning():
    """
    Clean data from telegram
    """

    # data extracted
    df_raw = read_data(PATH_TELEGRAM_RAW, "raw_telegram")

    # data already cleaned
    df_clean = read_data(PATH_TELEGRAM_CLEAN, "clean_telegram")

    # keep data not in clean data
    df = keep_data_to_process(df_raw, df_clean)

    # format date
    df = update_utc_date(df)

    # clean data
    df = clean_text_original(df)

    # update artifact
    for account in df["account"].unique():
        upd_data_artifact(
            f"Messages cleaned from {account}", df[df["account"] == account].shape[0]
        )

    # concat data
    df = concat_old_new_df(df_raw=df_clean, df_new=df, cols=["ID"])

    # save data
    save_data(PATH_TELEGRAM_CLEAN, "clean_telegram", df, ["account"])

    # create artifact
    create_artifact("dlk-flow-telegram-clean-artifact")
