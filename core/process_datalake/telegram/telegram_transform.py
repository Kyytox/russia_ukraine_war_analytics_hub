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
from tqdm import tqdm

import pandas as pd
from prefect import flow, task

# Variables
from core.config.paths import (
    PATH_TELEGRAM_CLEAN,
    PATH_TELEGRAM_TRANSFORM,
    PATH_SCRIPT_SERVICE_OLLAMA,
)
from core.config.variables import (
    SIZE_TO_TRANSLATE,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    get_telegram_accounts,
    concat_old_new_df,
    upd_data_artifact,
    create_artifact,
)

# IA
from core.libs.ollama_ia import ia_treat_message


@task(name="Translate data", task_run_name="translate-data")
def translate_data(df_source):
    """
    Translate data to English
    Translate only x messages because depending on your pc, it can take a long time

    Args:
        df_source: dataframe

    Returns:
        Dataframe with translated data
    """

    # sort dataframe by the number of words in text_original
    df_source["word_count"] = df_source["text_original"].str.split().str.len()
    df_source = df_source.sort_values(by="word_count", ascending=True).reset_index(
        drop=True
    )
    # print(f"Size sorted: {df_source}")
    print(
        f"Min: {df_source['word_count'].min()} - Max: {df_source['word_count'].max()}"
    )

    # keep only x messages
    df = df_source.loc[:SIZE_TO_TRANSLATE].copy()
    print(f"Size to translate: {df.shape}")

    # # translate text
    # for i, row in df.iterrows():
    #     print(f"Index: {i} - Id: {row['id_message']}")
    #     df.loc[i, "text_translate"] = ia_treat_message(
    #         row["text_original"], "translate"
    #     )

    group_size = 10
    df_result = pd.DataFrame()

    # Calcul number of batches
    total_batches = (len(df) + group_size - 1) // group_size

    # Create progress bar
    with tqdm(total=total_batches, desc=f"Translate data") as pbar:
        for i in range(0, len(df), group_size):
            df_group = df.loc[i : i + group_size].copy()

            # ask IA
            df_group.loc[:, "text_translate"] = df_group["text_original"].apply(
                lambda x: ia_treat_message(x, "translate")
            )

            df_result = pd.concat([df_result, df_group])

            # Update progress bar
            pbar.update(1)

    # remove col word_count
    df_result = df_result.drop(columns=["word_count"])

    return df_result


@task(
    name="Remove poorly translated data",
    task_run_name="remove-poorly-translated-data",
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
    if df.empty:
        return df

    df_poor_trans = df[
        (
            (
                df["text_translate"].str.split().str.len()
                / df["text_original"].str.split().str.len()
            )
            * 100
        ).round(2)
        < 65
    ]
    print(f"Size poorly translated data: {df_poor_trans.shape}")

    # keep 30 messages
    if df_poor_trans.shape[0] > 10:
        df_poor_trans = df_poor_trans.head(50)

        # remove data according to ID
        df = df[~df["ID"].isin(df_poor_trans["ID"])]
        print(f"Size after remove poorly translated data: {df.shape}")

    return df


@flow(
    name="Flow Single Telegram Transform",
    flow_run_name="flow-telegram-transform-{account}",
    log_prints=True,
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
    df_source = read_data(PATH_TELEGRAM_CLEAN, account)

    # read data transform
    df_transform = read_data(PATH_TELEGRAM_TRANSFORM, account)

    # remove poorly translated data
    df_transform = remove_poorly_translated_data(df_transform)

    # keep data not in transform data
    df = keep_data_to_process(df_source, df_transform)

    # reutrn if no data to process
    if df.empty:
        print("No data to transform")
        return
    print(f"Data to Transform: {df.shape[0]}")

    # update artifact
    upd_data_artifact(f"{account} - Messages to Transform", df.shape[0])

    # add col url
    df["url"] = "https://t.me/" + account + "/" + df["id_message"].astype(str)

    # translate data to english
    df = translate_data(df)

    # concat data
    df = concat_old_new_df(df_raw=df_transform, df_new=df, cols=[])

    # save data
    save_data(PATH_TELEGRAM_TRANSFORM, account, df)


@flow(
    name="Flow Master Telegram Transform",
    flow_run_name="flow-master-telegram-transform",
    log_prints=True,
)
def job_telegram_transform():
    """
    Process Telegram data
    """
    print("********************************")
    print("Start transforming Telegram")
    print("********************************")

    # start service ollama
    os.system(f"sh {PATH_SCRIPT_SERVICE_OLLAMA}")

    # get list of accounts
    list_accounts = get_telegram_accounts(PATH_TELEGRAM_CLEAN)

    # loop over accounts
    for account in list_accounts:
        print(f"Transform Account: {account}")
        process_transform(account)

    # create artifact
    create_artifact("flow-master-telegram-transform-artifact")
