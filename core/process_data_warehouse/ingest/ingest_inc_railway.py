# Process: Extract incident railway
# - Get source incident railway from google sheet
# - Check if the region is correct and exists in the list for graph Map
# - Check missing values in specific columns
# - if one of the previous steps fails, don't save the data
# - Save data


import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.variables import Variable
from prefect.states import Completed, Failed

# Variables
from core.config.paths import PATH_DWH_SOURCES
from core.config.variables import ID_EXCEL_INCIDENT_RAILWAY

# Functions
from core.libs.utils import (
    save_data,
    get_regions_geojson,
    create_artifact,
    upd_data_artifact,
)
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


dict_cols = {
    "Date": "date",
    "Region": "region",
    "Location": "location",
    "Gps": "gps",
    "Damaged Equipment": "dmg_eqp",
    "Incident Type": "inc_type",
    "Collision With": "coll_with",
    "Locomotive Damaged": "nb_loco_dmg",
    "Relay Damaged": "nb_relay_dmg",
    "Partisans Group": "prtsn_grp",
    "Partisans Arrest": "prtsn_arr",
    "Partisans Names": "prtsn_names",
    "Partisans Age": "prtsn_age",
    "Applicable Laws": "app_laws",
    "Source Links": "source_links",
    "Locomotive Damaged": "nb_loco_dmg",
    "Relay Damaged": "nb_relay_dmg",
    #
    "Sabotage Success": "sabotage_success",
    "Comments": "comments",
    "Exact Date": "exact_date",
}


@task(name="Rename columns", task_run_name="rename-columns")
def rename_cols(df):
    """
    Update the columns names

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    # format names
    df.columns = df.columns.str.strip()

    # rename columns
    return df.rename(columns=dict_cols)


@task(name="Remove columns", task_run_name="remove-columns")
def remove_cols(df):
    """
    Remove columns

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe without columns
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

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with updated type
    """

    df["date"] = pd.to_datetime(df["date"])

    # str to boolean
    df["prtsn_arr"] = df["prtsn_arr"].astype("bool")

    return df


@task(name="Format values", task_run_name="format-values")
def format_values(df):
    """
    Format the values in the columns

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with formatted values
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
    # get region from json file
    dict_region = get_regions_geojson()

    # add Month and Year
    df["month"] = df["date"].dt.month.astype(int)
    df["year"] = df["date"].dt.year.astype(int)

    # add id region
    df["id_region"] = df["region"].map(dict_region)

    return df


@task(
    name="Control columns",
    task_run_name="control-columns",
)
def ctrl_cols(df):
    """
    Check if the columns are correct

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    err = []

    # check if all columns are in the dataframe
    for col in dict_cols.values():
        if col not in df.columns:
            err.append(col)

    if len(err) > 0:
        return Failed(message=f"Columns incorrect: {err}")

    return Completed(message="Control columns OK")


@task(
    name="Control date",
    task_run_name="control-date",
)
def ctrl_date(df):
    """
    Check if the columns are correct

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    if df["date"].isnull().sum() > 0:
        return Failed(message="Date cannot be null")

    # find date incorrect
    incorrect_dates = df[~df["date"].str.match(r"^\d{1,2}/\d{1,2}/\d{4}$")]
    if not incorrect_dates.empty:
        return Failed(message=f"Date incorrect: {incorrect_dates.IDX.values}")

    return Completed(message="Control date OK")


@task(name="Control regions", task_run_name="control-regions")
def ctrl_regions(df, dict_region):
    """
    Check if the region is correct and exists in the list for graph Map

    Args:
        df (pd.DataFrame): Dataframe to control
        dict_region (dict): Dictionary of regions

    Returns:
        Completed or Failed
    """

    # get incorrect region
    list_incorrect = []
    for region in df["region"].unique():
        if region != None:
            if region not in dict_region.keys():
                list_incorrect.append(region)

    if "" in list_incorrect:
        return Completed(
            message=f"Region OK, but some are empty !! ( maybe no region specified )"
        )

    if len(list_incorrect) > 0:
        print("Region incorrect: ")
        return Failed(message=f"Region incorrect: {list_incorrect}")

    return Completed(message="Region OK")


@task(name="Control missing values", task_run_name="control-missing-values")
def ctrl_miss_val_solo_col(df):
    """
    Check missing values in solo specific column

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    list_cols = [
        "date",
        "dmg_eqp",
        "inc_type",
        "source_links",
    ]

    err = []

    # check missing values
    for col in list_cols:
        if df[col].isnull().sum() > 0 or sum(df[col] == "") > 0:
            err.append(col)
        else:
            print(f"No missing values in {col}")

    if len(err) > 0:
        return Failed(message=f"Columns {err} has missing values")

    return Completed(message=f"No missing values for solo columns")


@task(
    name="Control missing values type incident",
    task_run_name="control-missing-values-type-incident",
)
def ctrl_miss_val_set_cols(df):
    """
    Check missing values in combination of columns
    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    dict_cols = {
        "Sabotage": "prtsn_grp",
        "Collision": "coll_with",
    }

    err = []

    for inc_type, col in dict_cols.items():
        if df[df["inc_type"] == inc_type][col].isnull().sum() > 0:
            err.append(inc_type)
        else:
            print(f"No missing values in {inc_type} - {col}")

    if len(err) > 0:
        return Failed(message=f"Type incident {err} has missing values")
    return Completed(message="No missing values for combination of columns")


@task(name="Control partisans information", task_run_name="control-partisans-info")
def ctrl_missing_values_prtsn(df):
    """
    Check missing values in partisans related columns and verify data consistency

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """
    # Colonnes à vérifier pour les partisans sans affiliation
    partisans_cols = [
        "prtsn_age",
        "prtsn_arr",
        "prtsn_names",
        "app_laws",
    ]

    err = []

    # Check missing values for partisans with no affiliation
    no_affil_mask = df["prtsn_grp"] == "No affiliation"
    if no_affil_mask.any():
        missing_cols = [
            col for col in partisans_cols if df[no_affil_mask][col].isnull().any()
        ]
        if missing_cols:
            err.append(f"Missing values in {missing_cols} for No affiliation partisans")

    # Check consistency between number of partisans ages and names
    has_partisans = df["prtsn_names"].notna()
    if has_partisans.any():
        age_counts = df[has_partisans]["prtsn_age"].str.split(",").apply(len)
        name_counts = df[has_partisans]["prtsn_names"].str.split(",").apply(len)

        mismatched = age_counts != name_counts
        if mismatched.any():
            err.append("Mismatch between number of partisans ages and names")
            print("Rows with mismatched counts:", df[has_partisans][mismatched])

    if err:
        return Failed(message=f"Partisans information errors: {err}")

    return Completed(message="Partisans information validation OK")


@task(
    name="Control data quality",
    task_run_name="task-control-data-quality",
)
def control_data_quality(df):
    """
    Control the quality of the data

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        Completed or Failed
    """

    # get regions
    dict_region = get_regions_geojson()

    # control data
    st_col = ctrl_cols(df)
    st_dt = ctrl_date(df)
    st_reg = ctrl_regions(df, dict_region)
    st_miss_col = ctrl_miss_val_solo_col(df)
    st_miss_cols = ctrl_miss_val_set_cols(df)
    st_partisans = ctrl_missing_values_prtsn(df)

    # Check if all steps are completed
    top_completed = all(
        [
            st_col.is_completed(),
            st_dt.is_completed(),
            st_reg.is_completed(),
            st_miss_col.is_completed(),
            st_miss_cols.is_completed(),
            st_partisans.is_completed(),
        ]
    )

    # Update data artifact
    upd_data_artifact(f"Ingestion incident railway Completed", top_completed)
    upd_data_artifact(f"Control regions: {st_reg.result()}", st_reg.message)
    upd_data_artifact(f"Control date: {st_dt.result()}", st_dt.message)
    upd_data_artifact(
        f"Control missing values solo columns: {st_miss_col.result()}",
        st_miss_col.message,
    )
    upd_data_artifact(
        f"Control missing values set columns: {st_miss_cols.result()}",
        st_miss_cols.message,
    )
    upd_data_artifact(
        f"Control partisans information: {st_partisans.result()}",
        st_partisans.message,
    )

    if top_completed == False:
        return Failed(message="Ingestion incident railway Failed")

    return Completed(message="Ingestion incident railway Completed")


@flow(
    name="DWH Subflow Ingestion Inc Railway",
    flow_run_name="dwh-subflow-ingestion-inc-railway",
    log_prints=True,
)
def flow_ingest_incident_railway():
    """
    Retrieve data Incident Railway from Google Cloud
    Control quality of data
    """

    # vars google
    spreadsheet_id = ID_EXCEL_INCIDENT_RAILWAY
    range_name = "Incidents Russian Railway - DATA"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get data from google sheet
    df = get_sheet_data(service, spreadsheet_id, range_name)

    if df.empty:
        return Failed(message="Data is empty")

    # rename columns
    df = rename_cols(df)

    # control data
    st_data = control_data_quality(df)

    if st_data.is_failed():
        return Failed(message="Ingestion incident railway Failed")

    # Transform columns
    df = remove_cols(df)
    df = update_type(df)
    df = format_values(df)
    df = add_cols(df)

    #
    # create artifact
    create_artifact("dwh-subflow-ingestion-inc-railway-artifact")

    # save data
    save_data(PATH_DWH_SOURCES, "incidents_railway", df=df)

    return Completed(message="Ingestion incident railway Completed")
