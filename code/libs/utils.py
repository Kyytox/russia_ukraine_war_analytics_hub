import os
import pandas as pd


def read_data(base_path, file_name):
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


def save_data(base_path, file_name, df_old, df):
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

    print(f"Saving {df.shape} data to {base_path}/{file_name}.parquet")

    # save data to parquet
    df = pd.concat([df_old, df]).drop_duplicates().reset_index(drop=True)
    df.to_parquet(os.path.join(base_path, f"{file_name}.parquet"))

    return
