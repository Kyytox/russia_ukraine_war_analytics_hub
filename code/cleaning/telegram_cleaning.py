# Process Cleaning
# - get list of accounts
# - loop over accounts
# - read raw data
# - read data clean
# - keep data not in clean data
# - format date according to utc
# - clean data
# - save data


import re
import datetime
import pandas as pd

# Variables
from utils.variables import (
    list_accounts_telegram,
    path_telegram_raw,
    path_telegram_clean,
    dict_utc,
)

# Functions
from libs.utils import read_data, save_data


def keep_data_to_clean(df_raw, df_clean):
    """
    Filter data

    Args:
        df: dataframe
        df_clean: dataframe

    Returns:
        Dataframe with filtered data
    """

    # keep data not in clean data
    if df_clean.empty:
        df = df_raw
    else:
        df = df_raw[~df_raw["id_message"].isin(df_clean["id_message"])].reset_index(
            drop=True
        )
    print(f"Data to clean: {df.shape}")

    return df


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


def format_text(text):
    """
    Format text

    Args:
        text: text

    Returns:
        Formatted text
    """
    # print("text", text)

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
    text = regrex_pattern.sub(r" ", text)

    # remove
    text = re.sub(r"Подписывайся на SHOT", " ", text)
    text = re.sub(r"Прислать новость", " ", text)
    text = re.sub(r"Предложить свою новость", " ", text)
    text = re.sub(r"Подслушано электрички Москвы", " ", text)
    text = re.sub(r"#Новости@transport_online", " ", text)
    text = re.sub(r"Минтранс Подмосковья", " ", text)
    text = re.sub(r"Прислать фото/видео/информацию: @Astra4bot", " ", text)
    text = re.sub(r"Резервный канал ASTRA: https://t.me/astrapress2", " ", text)

    # remove after
    text = re.sub(r"\s*Фото\s*\S*\s*от:.*?\n", " ", text)
    text = re.sub(r"\s*Видео\s*\S*\s*от:.*?\n", " ", text)

    # remove words follow by :
    text = re.sub(r"@\S+", " ", text)
    text = re.sub(r"http\S+", " ", text)

    # remove all after
    text = re.sub(r"@Astra4botРезервный.*", " ", text)
    text = re.sub(r"Прислать фото/видео/информацию:", " ", text)

    #  —
    text = re.sub(r" — ", ". ", text)
    text = re.sub(r"— ", " ", text)

    # remove multiple spaces
    text = re.sub(r" +", " ", text).strip()

    # print("text after ", text)

    # if last caracter is ':', remove
    if text == "":
        return None

    if text[-1] == ":":
        text = text[:-1]

    return text


def clean_data(df):
    """
    Clean data

    Args:
        df: dataframe

    Returns:
        Dataframe with cleaned data
    """

    # format text
    df.loc[:, "text_original"] = df["text_original"].apply(format_text)

    # remove text None
    df = df.dropna(subset=["text_original"])

    return df


def telegram_cleaning():
    """
    Clean data from telegram
    """

    # get list of accounts
    for account in list_accounts_telegram:
        print("--------------------")
        print(account)

        # read raw data
        df_raw = read_data(path_telegram_raw, f"{account}")

        # read clean data
        df_clean = read_data(path_telegram_clean, f"{account}")

        # keep data not in clean data
        df = keep_data_to_clean(df_raw, df_clean)

        # format date
        df = format_date(df, account)

        # clean data
        df = clean_data(df)

        # save data
        save_data(path_telegram_clean, f"{account}", df_clean, df)
        exit()