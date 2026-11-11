import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.states import Completed, Failed
import kagglehub

# Variables
from core.config.paths import PATH_DWH_MILITARY_LOSSES
from core.config.variables import ID_EXCEL_RUSSIA_BLOCK_SITE
from core.config.dwh_corresp_schema import CORRESP_SCHEMA_MILITARY_LOSSES_RU_EQUIP
from core.config.dwh_schemas import DWH_SCHEMA_MILITARY_LOSSES_RU_EQUIP

# Functions
from core.libs.utils import (
    save_data,
    create_artifact,
    upd_data_artifact,
    rename_cols,
    keep_cols,
    retype_cols,
    drop_duplicates,
)
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


@task(name="Calculate daily destruction", task_run_name="calculate-daily-destruction")
def calculate_daily_destruction(df):
    """
    Calculate the daily destruction numbers

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with calculated daily equipment destruction numbers
    """
    # sort by date
    df = df.sort_values(by="day")

    # for each column, calculate the daily destruction numbers (because the data is cumulative)
    cols_to_calculate = [
        "aircraft",
        "helicopter",
        "tank",
        "apc",
        "field_artillery",
        "mrl",
        "drone",
        "naval_ship",
        "anti_aircraft_warfare",
        "special_equipment",
        "vehicles_and_fuel_tanks",
        "cruise_missiles",
        "submarines",
        "ground_robotic_systems",
    ]

    for col in cols_to_calculate:
        df[col + "_daily"] = df[col].diff().fillna(df[col]).fillna(0).astype(int)

    return df


@flow(
    name="DWH Subflow Ingestion Military Losses",
    flow_run_name="dwh-subflow-ingestion-military-losses",
    log_prints=True,
)
def flow_ingest_military_losses():
    """
    Integration of Russia blocked sites in the Data Warehouse
    """

    # Download latest version
    # path = kagglehub.dataset_download(
    #     "piterfm/2022-ukraine-russian-war",
    #     path="russia_losses_equipment.csv",
    #     output_dir=PATH_DWH_MILITARY_LOSSES,
    #     force_download=True,
    # )
    path = PATH_DWH_MILITARY_LOSSES + "/russia_losses_equipment.csv"

    # read data
    df = pd.read_csv(path, sep=",")

    # add month, year
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year

    # rename cols
    df = rename_cols(df, CORRESP_SCHEMA_MILITARY_LOSSES_RU_EQUIP)

    # keep cols needed
    df = keep_cols(df, CORRESP_SCHEMA_MILITARY_LOSSES_RU_EQUIP)

    # retype cols
    df = retype_cols(df, DWH_SCHEMA_MILITARY_LOSSES_RU_EQUIP)

    # calculate daily destruction numbers, because the data is cumulative
    df = calculate_daily_destruction(df)

    print(f"Dataframe shape: {df.shape}")
    print(f"Dataframe dtypes: {df.dtypes}")
    print(f"Dataframe head: {df.head()}")

    print(
        f"Dataframe columns: {df[['date', 'aircraft', 'aircraft_daily', 'helicopter', 'helicopter_daily', 'tank', 'tank_daily']].head(10)}"
    )

    exit()

    upd_data_artifact("Ingestion Military Losses", df.shape[0])

    # save data
    save_data(PATH_DWH_MILITARY_LOSSES, "russia_losses_equipment", df)

    # create artifact
    create_artifact("dwh-subflow-ingestion-military-losses")

    return Completed(message="Ingestion Military Losses completed")
