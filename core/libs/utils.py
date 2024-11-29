import os
import json
import pandas as pd
from prefect import flow, task
from prefect.runtime import task_run

# Variables
from core.utils.variables import list_accounts_telegram, path_json_ru_region


@task(name="Get telegram accounts", task_run_name="Get telegram accounts")
def get_telegram_accounts(path: str) -> list:
    """
    Get telegram accounts

    Args:
        path: path to accounts

    Returns:
        List of accounts
    """
    # get accounts
    list_accounts = [
        file_name.split(".")[0]
        for file_name in os.listdir(path)
        if file_name.endswith(".parquet")
    ]

    if not list_accounts:
        list_accounts = list_accounts_telegram

    print(f"Accounts: {list_accounts}")

    return list_accounts


def generate_task_name():
    """
    Generate task name

    Returns:
        Task name
    """
    # get task name
    task_name = task_run.get_name().split(" ")[0]

    # get parameters
    params = task_run.get_parameters()

    # get file name
    file = f"{params['base_path'].split('/')[-1]}/{params['file_name']}"
    return f"{task_name} {file}"


@task(
    name="Read data",
    task_run_name=generate_task_name,
    tags=["read"],
)
def read_data(base_path: str, file_name: str) -> pd.DataFrame:
    """
    Read data from parquet

    Args:
        base_path: base path
        file_name: file name

    Returns:
        Dataframe with parquet data
    """
    # get data
    try:
        df = pd.read_parquet(f"{base_path}/{file_name}.parquet")
    except Exception as e:
        print(e)
        df = pd.DataFrame()

    print(f"Reading {df.shape} data from {base_path}/{file_name}.parquet")
    return df


@task(name="Save data", task_run_name=generate_task_name, tags=["save"])
# def save_data(base_path, file_name, df):
def save_data(base_path: str, file_name: str, df: pd.DataFrame):
    """
    Save data to parquet

    Args:
        base_path: base path
        file_name: file name
        df_old: old dataframe
        df: dataframe to save

    Returns:
        None
    """

    if df.empty:
        print(f"No data to save")
        return

    # create folder if not exist
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    # save data to parquet
    print(f"Saving {df.shape} data to {base_path}/{file_name}.parquet")
    df.to_parquet(os.path.join(base_path, f"{file_name}.parquet"))

    return


@task(name="Filter data to process", task_run_name="Filter data to process")
def keep_data_to_process(
    df_source: pd.DataFrame, df_to_filter: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter data

    Args:
        df_source: dataframe
        df_to_filter: dataframe to filter

    Returns:
        Dataframe with filtered data
    """

    # keep data not in clean data
    if df_to_filter.empty:
        df = df_source
    else:
        df = df_source[
            ~df_source["id_message"].isin(df_to_filter["id_message"])
        ].reset_index(drop=True)

    return df


@task
# def concat_old_new_df(df_raw, df_new, cols):
def concat_old_new_df(
    df_raw: pd.DataFrame, df_new: pd.DataFrame, cols: list
) -> pd.DataFrame:
    """
    Concatenate old and new dataframes
    Drop duplicates by cols or not
    Sort by date

    Args:
        df_raw: old dataframe
        df_new: new dataframe
        cols: list of columns to drop duplicates

    Returns:
        df: dataframe
    """

    df = (
        pd.concat([df_raw, df_new])
        .drop_duplicates(subset=cols if len(cols) > 0 else None)
        .reset_index(drop=True)
        .sort_values("date")
    )
    return df


@task(name="Get region Géojson")
def get_regions_geojson():
    """
    Get the region and id from the json file
    """

    # read file json
    with open(path_json_ru_region) as file:
        data = json.load(file)

    # get id and name
    dict_region = {}
    for i in range(len(data["features"])):
        dict_region[data["features"][i]["properties"]["name"]] = data["features"][i][
            "id"
        ]

    # update dict
    dict_region = {
        k.replace("Moskva", "Moscow").replace("'", ""): v
        for k, v in dict_region.items()
    }

    return dict_region
