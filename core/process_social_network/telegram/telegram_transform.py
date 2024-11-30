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

# IA
from core.libs.ollama_ia import ia_treat_message


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
        df.loc[i, "text_translate"] = ia_treat_message(
            row["text_original"], "translate"
        )

    return df


@task(
    name="Remove data not translated correctly",
    task_run_name="Remove data not translated correctly",
)
def remove_poorly_translated_data(df):
    """
    Remove data not translated correctly
    Remove data where text_translate is 60% less than text_original

    Args:
        df: dataframe with translated data

    Returns:
        Dataframe with removed data
    """

    # remove data where text_translate is 60% less than text_original
    df1 = df[
        (
            (
                df["text_translate"].str.split().str.len()
                / df["text_original"].str.split().str.len()
            )
            * 100
        ).round(2)
        < 60
    ]

    print(f"Size after remove poorly translated data: {df1.shape}")

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

    # remove poorly translated data
    # df_transform = remove_poorly_translated_data(df_transform)

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
