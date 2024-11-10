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
from prefect import flow, task

# Variables
from code.utils.variables import (
    path_telegram_raw,
    path_telegram_clean,
    dict_utc,
)

# Functions
from code.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    get_telegram_accounts,
    concat_old_new_df,
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
    text = regrex_pattern.sub(r"", text)

    # remove
    text = re.sub(r"Подписывайся на SHOT", "", text)
    text = re.sub(r"Прислать новость", "", text)
    text = re.sub(r"Предложить свою новость", "", text)
    text = re.sub(r"Подслушано электрички Москвы", "", text)
    text = re.sub(r"#Новости@transport_online", "", text)
    text = re.sub(r"Минтранс Подмосковья", "", text)
    text = re.sub(r"Прислать фото/видео/информацию: @Astra4bot", "", text)
    text = re.sub(r"Резервный канал ASTRA: https://t.me/astrapress2", "", text)
    text = re.sub(r"astrapress@protonmail.com", "", text)
    text = re.sub(r"SHOT", "", text)

    # remove after
    text = re.sub(r"\s*Фото\s*\S*\s*от:.*?\n", "", text)
    text = re.sub(r"\s*Видео\s*\S*\s*от:.*?\n", "", text)

    # remove words follow by :
    text = re.sub(r"@\S+", "", text)
    text = re.sub(r"http\S+", "", text)

    # remove all after
    text = re.sub(r"@Astra4botРезервный.*", "", text)
    text = re.sub(r"Прислать фото/видео/информацию:", "", text)

    #  —
    text = re.sub(r" — ", ". ", text)
    text = re.sub(r"— ", "", text)

    # remove spaces
    text = re.sub(r" +", " ", text).strip()

    # if last caracter is ':', remove
    if text == "":
        print("text after cleaning is empty")
        return None

    # remove first caracter if is space
    if text[0] == " ":
        text = text[1:]

    if text[-1] == ":":
        text = text[:-1]

    return text


@task(name="Clean text")
def clean_text(df):
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
    df = clean_text(df)

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
