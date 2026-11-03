import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.states import Failed, Completed


# Variables
from core.config.paths import PATH_DWH_BRONZE_INC_RAILWAY, PATH_DWH_SILVER_INC_RAILWAY

# Functions
from core.libs.utils import get_regions_geojson
from core.common.parquet import read_parquet, save_parquet


@task(name="Remove columns", task_run_name="remove-columns")
def remove_cols(df):
    """
    Update the columns names
    """

    # remove cols
    df = df.drop(
        columns=[
            "comments",
            "exact_date",
            "sabotage_success",
            "nb_loco_dmg",
            "nb_relay_dmg",
        ]
    )

    return df


@task(name="Update type", task_run_name="update-type")
def update_type(df):
    """
    Update the data type
    """

    df["date"] = pd.to_datetime(df["date"])

    # str to boolean
    df["prtsn_arr"] = df["prtsn_arr"].astype("bool")

    return df


@task(name="Format values", task_run_name="format-values")
def format_values(df):
    """
    Format the values in the columns
    """

    # remove \n
    df["source_links"] = df["source_links"].str.replace("\n", "")

    # # remove spaces and . at the end of the string
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
        df[col] = df[col].str.rstrip(".")

    # Fill NaN values
    df = df.fillna(np.nan)

    return df


@task(name="Add columns", task_run_name="add-columns")
def add_cols(df):
    """
    Add columns
    """

    # add Month and Year
    df["month"] = df["date"].dt.month.astype(int)
    df["year"] = df["date"].dt.year.astype(int)

    # get region from json file
    dict_region = get_regions_geojson()

    # add id region
    df["id_region"] = df["region"].map(dict_region)

    return df


@flow(
    name="DWH Subflow Transform incidents railway",
    flow_run_name="dwh-subflow-transform-incidents-railway",
    log_prints=True,
)
def subflow_transform_incidents_railway():
    """
    Transform the data from the source
    """
    # get data from source
    df = read_parquet(PATH_DWH_BRONZE_INC_RAILWAY, "incidents_railway")
    if df.empty:
        return Failed(message="Data is empty")

    # update type
    df = update_type(df)

    # format values
    df = format_values(df)

    # add columns
    df = add_cols(df)

    # save data
    save_parquet(PATH_DWH_SILVER_INC_RAILWAY, "incidents_railway", df)
