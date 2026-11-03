import pandas as pd

from prefect import task


@task(name="Reorder columns", task_run_name="reorder-columns")
def reorder_cols(df, dict_cols):
    """
    Select and reorder columns in a dataframe based on a dictionary.

    Args:
        df (pd.DataFrame): Dataframe to control
        dict_cols (dict): Dictionary with columns and types

    Returns:
        pd.DataFrame: Dataframe with only the columns in the dict_cols
    """

    # keep only the columns in the dict_cols
    return df[[col for col in dict_cols.keys() if col in df.columns]]


@task(name="Retype columns", task_run_name="retype-columns")
def retype_cols(df, dict_types):
    """
    Retype columns in a dataframe based on a dictionary.

    Args:
        df (pd.DataFrame): Dataframe to control
        dict_types (dict): Dictionary with columns and types

    Returns:
        pd.DataFrame: Dataframe with retyped columns
    """

    for col, dtype in dict_types.items():
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                df[col] = df[col].astype(dtype)

    return df
