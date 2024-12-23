# Process
# - Get data source from data/warehouse
# - Rename columns
# - Check regions
# - Check missing values
# - Update type
# - Format values
# - Add columns (month, year, id_region)
# - Save data


import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.states import Failed


# Variables
from core.utils.variables import path_dw_sources, path_dw_transform

# Functions
from core.libs.utils import read_data, save_data, get_regions_geojson


dict_cols = {
    "Date": "date",
    "Region": "region",
    "Location": "location",
    "Gps": "gps",
    "Damaged Equipment": "dmg_eqp",
    "Incident Type": "inc_type",
    "Collision With": "coll_with",
    "Partisans Group": "prtsn_grp",
    "Partisans Reward": "prtsn_rwd",
    "Partisans Age": "prtsn_age",
    "Partisans Arrest": "prtsn_arr",
    "Partisans Names": "prtsn_names",
    "Applicable Laws": "app_laws",
    "Source Links": "source_links",
}


@task(name="Rename columns")
def rename_cols(df):
    """
    Update the columns names
    """

    # remove cols
    df = df.drop(columns=["Comment"])
    df = df.drop(columns=["Exact Date"])

    # remove spaces
    df.columns = df.columns.str.strip()

    # rename columns
    df = df.rename(columns=dict_cols)

    return df


@task(name="Update type")
def update_type(df):
    """
    Update the data type
    """

    df["date"] = pd.to_datetime(df["date"])

    print(df.dtypes)

    # str to boolean
    df["prtsn_rwd"] = df["prtsn_rwd"].replace("0", "").astype("bool")
    df["prtsn_arr"] = df["prtsn_arr"].replace("0", "").astype("bool")

    return df


@task(name="Format values")
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

    return df


@task(name="Add columns")
def add_cols(df):
    """
    Add columns
    """

    # add Month and Year
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year

    # get region from json file
    dict_region = get_regions_geojson()

    # add id region
    df["id_region"] = df["region"].map(dict_region)

    return df


@flow(name="Transform incident railway", log_prints=True)
def job_transform_incident_railway():
    """
    Transform the data from the source
    """

    # get data from source
    df = read_data(path_dw_sources, "incidents_railway")
    if df.empty:
        return Failed(message="Data is empty")

    # rename columns
    df = rename_cols(df)

    # update type
    df = update_type(df)

    # format values
    df = format_values(df)

    # add columns
    df = add_cols(df)

    # save data
    save_data(path_dw_transform, "incidents_railway", df=df)
