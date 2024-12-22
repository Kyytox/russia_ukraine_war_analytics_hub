# Process Cleaning
# - get data from raw
# - get data from clean
# - keep data to process
# - Format date
# - Format text
# - Save data

import re
import pandas as pd
from prefect import flow, task

# Variables
from core.utils.variables import (
    path_twitter_raw,
    path_twitter_clean,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    concat_old_new_df,
    format_clean_text,
)


@task(name="Format date")
def format_date(df):
    """
    Format date %a %b %d %H:%M:%S %z %Y to %Y-%m-%d %H:%M:%S
    ex: Mon Sep 27 20:03:46 +0000 2021 to 2021-09-27 20:03:46

    Args:
        df: DataFrame

    Returns:
        DataFrame
    """
    # format date
    df["date"] = pd.to_datetime(df["date"], format="%a %b %d %H:%M:%S %z %Y")
    df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # convert to datetime
    df["date"] = pd.to_datetime(df["date"])

    print(df.dtypes)

    return df


@flow(name="Flow Twitter Cleaning", log_prints=True)
def job_twitter_cleaning():
    """
    Clean data from Twitter
    """

    # get raw data
    df_raw = read_data(path_twitter_raw, "twitter")

    # get clean data
    df_clean = read_data(path_twitter_clean, "twitter")

    # keep data to process
    df = keep_data_to_process(df_raw, df_clean)

    # format date
    df = format_date(df)

    # format text
    df["text_original"] = df["text_original"].apply(format_clean_text)

    # concat data
    df = concat_old_new_df(df_clean, df, cols=["id_message"])

    # save data
    save_data(path_twitter_clean, "twitter", df)
