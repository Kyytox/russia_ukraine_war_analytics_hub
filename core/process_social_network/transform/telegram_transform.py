# Process Cleaning
# - Start service ollama
# - get list of accounts
# - loop over accounts
# - read data clean
# - read data transform
# - keep data not in transform data
# - add col url
# - translate data to english
# - save data

import os
import re

from prefect import flow, task

# ia for translate
import ollama

# Variables
from core.utils.variables import (
    path_telegram_clean,
    path_telegram_transform,
    path_script_service_ollama,
    size_to_translate,
    prefix_message_ia,
    ia_name,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    get_telegram_accounts,
    concat_old_new_df,
)


def clean_text(text):
    """
    Clean text

    Args:
        text: text

    Returns:
        Cleaned text
    """

    list_replace = [
        "Here is the translation:",
        "Here is the English translation: ",
        "Here is the translation to English:",
        "Here's the translation:",
        "Translation in English:",
        "I'm ready to translate.",
        "The translation is:",
        "Translation:",
        "I'll translate the text for you:",
        "I'll translate the text accurately,",
        "Translation in English as listed:",
        "I can translate this text for you.",
        "I'll translate the text accurately from Russian to English while maintaining the context and tone of the original.",
        "I'll translate the text accurately and grammatically correct.",
        "Here is the translation of your text from Russian to English:",
    ]

    # remove list_replace
    for replace in list_replace:
        text = text.replace(replace, "")

    # remove all after Subscribe to SHOT
    text = re.sub(r"Subscribe to SHOT.*", "", text)

    text = re.sub(r" +", " ", text).strip()
    # text = text.replace("\n", " ")

    return text


def translate_text(text_original):
    """
    Translate text

    Args:
        row: row of dataframe

    Returns:
        Translated text
    """

    # add prefix
    text = prefix_message_ia + text_original

    try:
        # translate text
        response = ollama.chat(
            model=ia_name,
            messages=[
                {"role": "user", "content": text},
            ],
        )

        # clean text
        return clean_text(response["message"]["content"])
    except Exception as e:
        print(f"Error: {e}")
        if "111" in str(e):
            print("Ollama Not Active")
            exit()

    return ""


@task(name="Translate data")
def translate_data(df_source):
    """
    Translate data to English
    Translate only x messages because depending on your pc, it can take a long time

    Args:
        df_source: dataframe

    Returns:
        Dataframe with translated data
    """

    # keep only x messages
    df = df_source.loc[:size_to_translate].copy()
    print(f"Size to translate: {df.shape}")

    # translate text
    for i, row in df.iterrows():
        print(f"Index: {i} - Id: {row['id_message']}")
        df.loc[i, "text_translate"] = translate_text(row["text_original"])

    return df


@task(
    name="Process Transform",
    task_run_name="Process Transform for {account}",
)
def process_transform(account):
    """
    Process Transform

    Args:
        account: account name

    Returns:
        None
    """

    # read data clean
    df_source = read_data(path_telegram_clean, account)

    # read data transform
    df_transform = read_data(path_telegram_transform, account)

    # keep data not in transform data
    df = keep_data_to_process(df_source, df_transform)

    # reutrn if no data to process
    if df.empty:
        print("No data to transform")
        return
    print(f"Data to Transform: {df.shape[0]}")

    # add col url
    df["url"] = "https://t.me/" + account + "/" + df["id_message"].astype(str)

    # translate data to english
    df = translate_data(df)

    # concat data
    df = concat_old_new_df(df_raw=df_transform, df_new=df, cols=[])

    # save data
    save_data(path_telegram_transform, account, df)


@flow(
    name="Flow Telegram Transform",
    log_prints=True,
)
def job_telegram_transform():
    """
    Process Telegram data
    """

    # start service ollama
    os.system(f"sh {path_script_service_ollama}")

    # get list of accounts
    list_accounts = get_telegram_accounts(path_telegram_clean)

    # loop over accounts
    for account in list_accounts:
        print("########################################")
        print(f"Transform Account: {account}")

        process_transform(account)