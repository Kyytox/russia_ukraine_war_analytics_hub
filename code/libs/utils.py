import os
import pandas as pd


def get_telegram_accounts(path):
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
    print(f"Accounts: {list_accounts}")

    return list_accounts


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


def save_data(base_path, file_name, df_old=None, df=None):
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

    # concat data
    if df_old is not None:
        df = (
            pd.concat([df_old, df])
            .drop_duplicates()
            .reset_index(drop=True)
            .sort_values("date")
        )

    # save data to parquet
    df.to_parquet(os.path.join(base_path, f"{file_name}.parquet"))

    return


def keep_data_to_process(df_source, df_to_filter):
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
    print(f"Data to process: {df.shape}")

    return df
