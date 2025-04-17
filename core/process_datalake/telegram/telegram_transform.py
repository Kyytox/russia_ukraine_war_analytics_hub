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
    GROUP_SIZE_TO_TRANSLATE,
    SIZE_TO_TRANSLATE,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    keep_data_to_process,
    concat_old_new_df,
    upd_data_artifact,
    create_artifact,
)

# IA
from core.libs.ollama_ia import ia_treat_message


@task(name="Translate data", task_run_name="translate-data")
def translate_data(df_to_trt):
    """
    Translate data to English
    Translate only x messages because depending on your pc, it can take a long time

    Args:
        df_to_trt: dataframe to translate

    Returns:
        Dataframe with translated data
    """

    # keep only x messages
    df = df_to_trt.loc[:SIZE_TO_TRANSLATE].copy()
    print(f"Size to translate: {df.shape}")

    df_result = pd.DataFrame()

    # Calcul number of batches
    total_batches = (len(df) + GROUP_SIZE_TO_TRANSLATE - 1) // GROUP_SIZE_TO_TRANSLATE

    # Create progress bar
    with tqdm(total=total_batches, desc=f"Translate data") as pbar:
        for i in range(0, len(df), GROUP_SIZE_TO_TRANSLATE):
            df_group = df.loc[i : i + GROUP_SIZE_TO_TRANSLATE].copy()

            # ask IA
            df_group.loc[:, "text_translate"] = df_group["text_original"].apply(
                lambda x: ia_treat_message(x, "translate")
            )

            df_result = pd.concat([df_result, df_group])

            # Update progress bar
            pbar.update(1)

    return df_result


@task(name="Sort by number of words", task_run_name="sort-by-number-of-words")
def sort_by_nb_words(df):
    """
    Sort dataframe by the number of words in text_original
    For translate text not too long

    Args:
        df: dataframe

    Returns:
        Dataframe sorted
    """
    # add, sort and remove column
    df.loc[:, "word_nb"] = df["text_original"].str.split().str.len()
    df = df.sort_values(by="word_nb", ascending=True).reset_index(drop=True)

    print(f"Min: {df['word_nb'].min()} - Max: {df['word_nb'].max()} - Size: {df.shape}")
    df = df.drop(columns=["word_nb"])

    return df


@task(
    name="Remove poorly translated data",
    task_run_name="remove-poorly-translated-data",
)
def remove_poorly_translated_data(df):
    """
    Remove data not translated correctly
    Remove data where text_translate is 65% less than text_original
    We do this to retranslate the data again
    So we remove the data from data already translated

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

    # keep 50 messages beaaause it's too long to retranslate if we have a lot of data
    if df_poor_trans.shape[0] > 50:
        df_poor_trans = df_poor_trans.head(50)

        # remove data according to ID
        df = df[~df["ID"].isin(df_poor_trans["ID"])]
        print(f"Size after remove poorly translated data: {df.shape}")

    return df


@flow(
    name="DLK Subflow Telegram Transform",
    flow_run_name="dlk-subflow-telegram-transform-{account}",
    log_prints=True,
)
def process_transform(df: pd.DataFrame, account: str) -> pd.DataFrame:
    """
    Process Transform

    Args:
        df: dataframe to transform
        account: account name

    Returns:
        Dataframe with transformed data
    """

    print(f"Data to Transform: {df.shape[0]}")

    # update artifact
    upd_data_artifact(f"Messages to Transform - {account}", df.shape[0])

    # add col url
    df.loc[:, "url"] = "https://t.me/" + account + "/" + df["id_message"].astype(str)

    # sort by nb words
    df = sort_by_nb_words(df)

    # translate data to english
    df = translate_data(df)

    return df


@flow(
    name="DLK Flow Telegram Transform",
    flow_run_name="dlk-flow-telegram-transform",
    log_prints=True,
)
def flow_telegram_transform():
    """
    Process Telegram data
    """

    # start service ollama
    os.system(f"sh {PATH_SCRIPT_SERVICE_OLLAMA}")

    """
    Init data
    """
    # read data clean
    df_clean = read_data(PATH_TELEGRAM_CLEAN, "clean_telegram")

    # read data transform
    df_old_transf = read_data(PATH_TELEGRAM_TRANSFORM, "transform_telegram")

    # remove poorly translated data for translate again
    df_transform = remove_poorly_translated_data(df_old_transf)

    # keep data not transformed
    df = keep_data_to_process(df_clean, df_old_transf)

    if df.empty:
        print("No data to transform")
        return

    """
    Process Transform
    """
    # init df
    df_final = pd.DataFrame()

    # loop over accounts
    for account in df["account"].unique():
        print(f"Transforming {account}")

        # get data for account
        df_acc = df[df["account"] == account]

        # process transform
        df_acc = process_transform(df_acc, account)

        # concat
        df_final = pd.concat([df_final, df_acc])

    """
    Save data
    """
    # concat final
    df_final = concat_old_new_df(df_raw=df_transform, df_new=df_final, cols=["ID"])
    print(f"Final data shape: {df_final.shape}")

    # save data
    save_data(PATH_TELEGRAM_TRANSFORM, "transform_telegram", df_final, ["account"])

    # create artifact
    create_artifact("dlk-flow-telegram-transform-artifact")
