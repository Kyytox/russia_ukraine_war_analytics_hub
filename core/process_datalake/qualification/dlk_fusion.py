import pandas as pd
from prefect import flow, task

# Functions
from core.libs.utils import (
    read_data,
    create_artifact,
)

from core.libs.duckdb_utils import insert_data_into_duckdb

# Variables
from core.config.paths import (
    PATH_TELEGRAM_TRANSFORM,
    PATH_TWITTER_CLEAN,
)


@task(name="Get Telegram data", task_run_name="get-telegram-data")
def get_telegram_data():
    """
    Get Telegram data

    Returns:
        Dataframe with Telegram data
    """

    # read data transform
    df_telegram = read_data(PATH_TELEGRAM_TRANSFORM, "transform_telegram")

    # convert to str
    df_telegram["id_message"] = df_telegram["id_message"].astype(str)

    return df_telegram


@task(name="Get Twitter data", task_run_name="get-twitter-data")
def get_twitter_data():
    """
    Get Twitter data

    Returns:
        Dataframe with Twitter data
    """

    # read data from clean
    return read_data(PATH_TWITTER_CLEAN, "twitter")


@task(name="Regroup data", task_run_name="regroup-data")
def regroup_data(df_telegram, df_twitter):
    """
    Regroup data

    Args:
        df_telegram: dataframe with Telegram data
        df_twitter: dataframe with Twitter data

    Returns:
        Dataframe with regrouped data
    """

    # group data
    df = pd.concat([df_telegram, df_twitter]).sort_values("date").reset_index(drop=True)

    # remove account, id_message
    # rename ID and date
    df = df.drop(columns=["account", "id_message"]).rename(
        columns={"ID": "id_message", "date": "date_message"}
    )

    # select
    return df[["id_message", "date_message", "url", "text_original", "text_translate"]]


@task(name="Remove data not pertinant", task_run_name="remove-data-not-pertinant")
def remove_data_not_pertinant(df):
    """
    Remove data not pertinant

    Args:
        df: dataframe with data

    Returns:
        Dataframe with data not pertinant
    """

    list_words = [
        "COVID",
        "covid",
    ]

    # remove data containing words (except text_translate is null)
    mask = pd.notna(df["text_translate"]) & df["text_translate"].str.contains(
        "|".join(list_words)
    )

    # remove data
    return df[~mask].reset_index(drop=True)


@flow(
    name="DLK SubFlow Fusion",
    flow_run_name="dlk-subflow-fusion",
    log_prints=True,
)
def subflow_datalake_fusion():
    """
    Subflow Datalake Fusion

    1. Get Telegram data
    2. Get Twitter data
    3. Regroup data
    4. Remove data not pertinant
    5. Insert data into DuckDB (no duplicates)
    """

    # get Telegram data
    df_telegram = get_telegram_data()

    # get Twitter data
    df_twitter = get_twitter_data()

    # regroup data
    df = regroup_data(df_telegram, df_twitter)

    # remove Data not pertinant
    df = remove_data_not_pertinant(df)

    # Insert data into DuckDB (no duplicates)
    query = """
        INSERT OR IGNORE INTO messages (id_message, date_message, url, text_original, text_translate)
        SELECT id_message, date_message, url, text_original, text_translate
        FROM df
        RETURNING 1
    """

    # Insert in messages table
    result_msg = insert_data_into_duckdb(df, query)
    print(f"Total New Data Inserted in messages: {result_msg}")

    # Insert in messages_theme table
    query = """
        INSERT OR IGNORE INTO messages_theme (id_message)
        SELECT id_message
        FROM df
        RETURNING 1
    """

    result_mt = insert_data_into_duckdb(df, query)
    print(f"Total New Data Inserted in messages_theme: {result_mt}")

    # create artifact (table with 2 line)
    create_artifact(
        "dlk-subflow-fusion",
        "table",
        [
            {"name": "Total New Data Inserted in messages", "value": result_msg},
            {"name": "Total New Data Inserted in messages_theme", "value": result_mt},
        ],
    )
