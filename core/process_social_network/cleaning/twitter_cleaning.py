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


def format_text(text):
    """
    Format text

    Args:
        text: text

    Returns:
        Formatted text
    """
    # remove special characters
    regrex_pattern = re.compile(
        pattern="["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+",
        flags=re.UNICODE,
    )
    text = regrex_pattern.sub(r"", text)

    # remove words follow by :
    text = re.sub(r"@\S+", "", text)
    text = re.sub(r"http\S+", "", text)

    # remove
    text = re.sub(r"&amp;", "", text)

    # remove spaces
    text = re.sub(r" +", " ", text).strip()

    return text


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
    df["text_original"] = df["text_original"].apply(format_text)

    # concat data
    df = concat_old_new_df(df_raw, df, cols=["id_message"])

    # save data
    save_data(path_twitter_clean, "twitter", df)
