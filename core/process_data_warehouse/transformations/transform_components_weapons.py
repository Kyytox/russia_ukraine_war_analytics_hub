import pandas as pd

from prefect import flow, task
from prefect.states import Completed, Failed

# Variables
from core.config.paths import (
    PATH_DWH_BRONZE_COMPO_WEAPONS,
    PATH_DWH_SILVER_COMPO_WEAPONS,
)

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    create_artifact,
)


from core.common.parquet import read_parquet, save_parquet
from core.common.dataframe import reorder_cols, retype_cols


@task(
    name="Add types weapons and equipment",
    task_run_name="task-add-types-weapons-equipment",
)
def add_types_weapons_equipment(df):
    """
    Add types weapons and equipment
    Take new data when merging (_y)

    Args:
        df: DataFrame

    Returns:
        df: DataFrame with types weapons and equipment
    """
    # read iles categories
    df_categories = pd.read_csv(
        f"{PATH_DWH_SILVER_COMPO_WEAPONS}/components_weapons_types.csv"
    )
    # df_categories = df_categories.drop(
    # columns=["Unnamed: 4", "Unnamed: 5", "weapon_country"]
    # )

    # select only needed columns
    df_categories = df_categories[
        ["weapon_details", "weapon", "weapon_type", "type_equipment"]
    ]

    # merge with categories
    df = df.merge(df_categories, on="weapon_details", how="left")

    return df


@task(
    name="Check if weapons are in types",
    task_run_name="task-check-weapons-in-types",
)
def check_weapons_in_types(df):
    """
    Check if weapons are in types

    Args:
        df: DataFrame

    Returns:
        bool: True if all weapons are in types, False otherwise
    """
    # check if one weapon is not in types
    list_types = ["type_equipment", "weapon_type", "weapon"]

    for col in list_types:
        if df[col].isnull().sum() > 0:
            print(f"At least one weapon is not in {col}")
            missing_weapons = df[df[col].isnull()]["weapon_details"].unique().tolist()
            print(f"Missing weapons in {col}:")
            print("\n".join(missing_weapons))
            print(f"Update components_weapons_types.csv with new weapons")
            return missing_weapons, False

    return [], True


@task(
    name="Replace values",
    task_run_name="task-replace-values",
)
def replace_values(df):
    """
    Format values

    Args:
        df: DataFrame to format

    Returns:
        df: DataFrame with formatted values
    """
    country_mapping = {
        "United States of America": "USA",
        "People's Republic of China": "China",
        "People's Republic of China, Taiwan": "Taiwan",
        "russian federation": "Russia",
        "": "Not Identified",
        None: "Not Identified",
    }

    # replace values
    df["manufacturer_country"] = df["manufacturer_country"].replace(country_mapping)

    return df


@task(
    name="Clean data",
    task_run_name="task-clean-data",
)
def clean_data(df):
    """
    Clean data

    Args:
        df: DataFrame to clean

    Returns:
        df: DataFrame with cleaned values
    """

    # remove /n in ref
    df["refs"] = df["refs"].str.replace("\n", "", regex=True).str.strip()

    # revome from weapon
    df["weapon_details"] = (
        df["weapon_details"]
        .str.replace(" (157 components)", "", regex=False)
        .str.strip()
    )

    # ingest_date to datetime
    df["ingest_date"] = pd.to_datetime(df["ingest_date"], errors="coerce")

    return df


@flow(
    name="DWH Subflow Transform Components Weapons",
    flow_run_name="dwh-subflow-transform-components-weapons",
    log_prints=True,
)
def subflow_transform_components_weapons():
    """
    Subflow Transform Components Weapons

    - Read data from bronze
    - Clean data
    - Replace values
    - Add types weapons and equipment
    - Check if weapons are in types
    - Update types
    - Save data to silver
    """

    # read source
    file_name = "components_weapons"
    # df = read_data(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name)
    df = read_parquet(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name)

    # Clean data
    df = clean_data(df)

    # Replace values
    df = replace_values(df)

    # Add types weapons and equipment
    df = add_types_weapons_equipment(df)

    # Check if weapons are in types
    list_miss, top_missing = check_weapons_in_types(df)

    if not top_missing:
        artifact_data = [
            {"weapon_details": item, "status": "missing"} for item in list_miss
        ]
        create_artifact(
            "dwh-subflow-transform-components-weapons-art", "table", artifact_data
        )
        return Failed(message="At least one weapon is not in types")

    # update types
    dict_types = {
        "id": "int64",
        "product": "string",
        "refs": "string",
        "weapon": "string",
        "weapon_details": "string",
        "weapon_type": "string",
        "type_equipment": "string",
        "manufacturer_country": "string",
        "manufacturer": "string",
        "ingest_date": "datetime64[ns]",
    }

    # reorder columns
    df = reorder_cols(df, dict_types)

    # retype columns
    df = retype_cols(df, dict_types)

    # save data
    # save_data(PATH_DWH_SILVER_COMPO_WEAPONS, file_name, df)
    save_parquet(PATH_DWH_SILVER_COMPO_WEAPONS, file_name, df)

    return Completed(message="Transform Components Weapons completed successfully")
