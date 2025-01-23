# Process
# - get filter data Telegram
# - get filter data Twitter
# - concat data
# - get data to classify (Excel from Google Drive)
# - convert columns
# - merge data (filter and data to classify)
# - clean columns
# - update data in Google Drive

import pandas as pd

from prefect import flow, task

# ia for classify
import ollama

# Functions
from core.libs.ollama_ia import ia_treat_message
from core.libs.utils import read_data
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from core.config.paths import (
    PATH_TELEGRAM_FILTER,
    PATH_TWITTER_FILTER,
    # id_excel_inc_railway,
)


@task(name="Merge filter to news", task_run_name="merge-filter-to-news")
def merge_filter_to_news(df_filter, df_to_classify):
    """
    Merge data
    Add new data potential news about railways

    Args:
        df_filter: dataframe with filter data
        df_to_classify: dataframe with news data

    Returns:
        Dataframe with merge data
    """
    print(f"Filter data: {df_filter.shape}")
    print(f"Classify data: {df_to_classify.shape}")

    print(
        f"Data integrated to data final: {df_to_classify[df_to_classify['is_add_to_final_dataset']].shape}"
    )

    print(
        f"Data classified not incident: {df_to_classify[~df_to_classify['is_incident_railway']].shape}"
    )

    # merge data
    df = pd.merge(
        df_filter,
        df_to_classify,
        how="outer",
        on=["account", "id_message"],
    )
    print(f"Data merged: {df.shape}")

    return df


@task(name="Clean columns", task_run_name="clean-columns")
def clean_columns(df):
    """
    Clean columns
    - adapt columns names for news data
    - adapt columns names for filter data
    - rename columns
    - drop cols _x and _y

    Args:
        df: dataframe with all data merged

    Returns:
        df: dataframe with clean columns

    """

    # fillna col _x by col _y
    df["date_x"] = df["date_x"].fillna(df["date_y"])
    df["text_original_x"] = df["text_original_x"].fillna(df["text_original_y"])
    df["text_translate_x"] = df["text_translate_x"].fillna(df["text_translate_y"])
    df["url_x"] = df["url_x"].fillna(df["url_y"])

    # fillna bool cols
    df["is_add_to_final_dataset"] = df["is_add_to_final_dataset"].fillna(False)
    df["is_incident_railway"] = df["is_incident_railway"].fillna(True)

    # remove cols _y
    df = df.drop(columns=[col for col in df.columns if col.endswith("_y")])

    # rename cols _x
    df = df.rename(columns={col: col.replace("_x", "") for col in df.columns})

    # reorder
    df = df[
        [
            "account",
            "id_message",
            "date",
            "text_original",
            "text_translate",
            "is_incident_railway",
            "response_ia",
            "is_add_to_final_dataset",
            "url",
        ]
    ].sort_values("date")

    # convert id to str
    df["id_message"] = df["id_message"].astype(str)

    # drop duplicates
    df = df.drop_duplicates(subset=["account", "id_message"], keep="last")

    print(f"Messages to classify: {df.shape}")
    return df


@task(name="Convert columns", task_run_name="convert-columns")
def convert_cols(df):
    """
    Convert columns
    """
    # convert str to bool
    df["is_add_to_final_dataset"] = df["is_add_to_final_dataset"] == "TRUE"
    df["is_incident_railway"] = df["is_incident_railway"] == "TRUE"

    # convert date
    df["date"] = pd.to_datetime(df["date"])

    # count number of None in id_message
    print(f"Number of None in id_message: {df['id_message'].isnull().sum()}")

    # convert id_message to int
    df["id_message"] = df["id_message"].astype(int)

    return df


@task(name="Concat data Sources", task_run_name="concat-data-sources")
def concat_data_sources(df_telegram_filter, df_twitter_filter):
    """
    Concat data sources
    """

    # convert to int
    df_twitter_filter["id_message"] = df_twitter_filter["id_message"].astype(int)

    # concat data
    df_filter = pd.concat([df_telegram_filter, df_twitter_filter], ignore_index=True)

    # replace nan by "" in text_translate
    df_filter["text_translate"] = df_filter["text_translate"].fillna("")

    return df_filter


@task(name="Classify with IA", task_run_name="classify-with-ia")
def classify_with_ia(df):
    """
    Classify with IA
    """
    print(f"Data to classify: {df.shape}")

    # get data to classify with ia, only 300 for avoid errors
    df_to_classify = (
        df[
            (df["is_add_to_final_dataset"] == False)
            & (df["is_incident_railway"])
            & (df["text_translate"] != "")
            & (df["response_ia"].isnull() | (df["response_ia"] == ""))
        ]
        .head(200)
        .reset_index(drop=True)
    )

    print(f"Data to classify with IA: {df_to_classify.shape}")

    for i, row in df_to_classify.iterrows():
        # ask IA
        print(f"Index: {i}")
        df_to_classify.loc[i, "response_ia"] = ia_treat_message(
            row["text_translate"], "classify"
        )

    print(df_to_classify)

    # concat data
    df_concat = (
        (pd.concat([df, df_to_classify], ignore_index=True))
        .drop_duplicates(subset=["account", "id_message"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # replace Nan
    df_concat["response_ia"] = df_concat["response_ia"].fillna("")

    print(f"Data classified: {df_concat.shape}")

    return df_concat


@flow(name="Filter to classify", log_prints=True)
def job_filter_to_classify():
    """
    Filter data to classify
    """

    spreadsheet_id = id_excel_inc_railway
    range_name = "news_railway_to_classified"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get telegram filter data
    df_telegram_filter = read_data(PATH_TELEGRAM_FILTER, "incidents_railway")

    # get twitter filter data
    df_twitter_filter = read_data(PATH_TWITTER_FILTER, "incidents_railway")

    # concat data sources
    df_filter = concat_data_sources(df_telegram_filter, df_twitter_filter)

    # get data from Google Sheet
    df_to_classify = get_sheet_data(service, spreadsheet_id, range_name)

    # convert columns
    df_to_classify = convert_cols(df_to_classify)

    # merge data
    df = merge_filter_to_news(df_filter, df_to_classify)

    # clean columns
    df = clean_columns(df)

    # classify with IA
    df = classify_with_ia(df)

    # save data
    df.to_csv("test_classify.csv", index=False)
    try:
        update_sheet_data(service, spreadsheet_id, range_name, df)
    except Exception as e:
        print("Retry Update", e)
        # connect to google sheet
        service = connect_google_sheet_api()
        update_sheet_data(service, spreadsheet_id, range_name, df)
