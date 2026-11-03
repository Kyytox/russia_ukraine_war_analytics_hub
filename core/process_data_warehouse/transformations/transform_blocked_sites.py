import pandas as pd
import numpy as np
from prefect import flow, task
from prefect.states import Completed

# Variables
from core.config.paths import (
    PATH_DWH_BRONZE_BLOCKED_SITES,
    PATH_DWH_SILVER_BLOCKED_SITES,
)
from core.config.dwh_corresp_schema import CORRESP_SCHEMA_RUSSIA_BLOCK_SITES

# Functions
from core.libs.utils import (
    rename_cols,
    drop_duplicates,
)

from core.common.parquet import read_parquet, save_parquet


@task(name="Format values", task_run_name="format-values")
def format_values(df):
    """
    Format the values in the columns

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with formatted values
    """

    # replcae '' and '-' by np.nan
    df["subcategory"] = df["subcategory"].replace(["", "-"], np.nan)

    # convert date
    df["date_blocked"] = pd.to_datetime(df["date_blocked"], errors="coerce")

    # replace all subcategory startwith 'Mirror' by 'Mirror Domains'
    df["subcategory"] = df["subcategory"].apply(
        lambda x: (
            "Mirror Domains" if isinstance(x, str) and x.startswith("Mirror") else x
        )
    )

    # put Uppercase to first letter of each word in subcategory
    df["subcategory"] = df["subcategory"].apply(
        lambda x: x.title() if isinstance(x, str) else x
    )

    # Replace United States by USA in country_domain
    df["country_domain"] = df["country_domain"].replace("United States", "USA")

    # if subcategory is null, replace by category
    df["subcategory"] = df.apply(
        lambda row: (
            row["category"] if pd.isnull(row["subcategory"]) else row["subcategory"]
        ),
        axis=1,
    )

    return df


@flow(
    name="DWH Subflow Transform blocked Sites",
    flow_run_name="dwh-subflow-transform-blocked-sites",
    log_prints=True,
)
def subflow_transform_blocked_sites():
    """
    Integration of blocked sites in RUssia in the Data Warehouse
    """

    # get raw data
    df = read_parquet(PATH_DWH_BRONZE_BLOCKED_SITES, "blocked_sites")

    # rename cols
    df = rename_cols(df, CORRESP_SCHEMA_RUSSIA_BLOCK_SITES)

    # Format values
    df = format_values(df)

    # add month, year
    df["month"] = df["date_blocked"].dt.to_period("M").dt.to_timestamp()
    df["year"] = df["date_blocked"].dt.year

    # drop duplicates
    df = drop_duplicates(df, ["domain"])

    # save data
    save_parquet(PATH_DWH_SILVER_BLOCKED_SITES, "blocked_sites", df)

    return Completed(message="Ingestion Blocked Sites completed")
