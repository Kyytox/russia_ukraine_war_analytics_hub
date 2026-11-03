import pandas as pd

from prefect import flow, task
from prefect.states import Completed


# Variables
from core.config.paths import (
    PATH_DWH_SILVER_COMPO_WEAPONS,
    PATH_DWH_GOLD,
)

from core.config.variables import (
    SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS,
)

# Utils
from core.libs.duckdb_utils import rewrite_duckdb_table
from core.common.parquet import read_parquet
from core.common.dataframe import reorder_cols, retype_cols


@task(
    name="Datamarts Global Count",
    task_run_name="dmt-global-count",
)
def dmt_global_count(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datamarts Global Count
    Count the number of occurrences of each value in the columns of a DataFrame

    Args:
        df: DataFrame

    Returns:
        DataFrame with count aggregations
    """

    count_columns = [
        "weapon",
        "type_equipment",
        "weapon_type",
        "manufacturer_country",
        "manufacturer",
    ]

    # Create list of Series with counts
    counts = [
        df[col]
        .value_counts()
        .reset_index(name="count")
        .rename(columns={col: "metric"})
        .assign(libelle=col)
        for col in count_columns
    ]

    # Concatenate all counts at once
    df_final = pd.concat(counts, ignore_index=True)[["libelle", "metric", "count"]]

    # Update types
    dict_types = {
        "libelle": "string",
        "metric": "string",
        "count": "int64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(
    name="Datamarts TreeMap Country Weapon Type",
    task_run_name="dmt-treemap-country-weapon-type",
)
def dmt_treemap_country_weapon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datamarts TreeMap Country Weapon
    Count the number of occurrences of each value in the columns of a DataFrame

    Args:
        df: DataFrame

    Returns:
        DataFrame with count aggregations
    """

    # group by country and get top 10
    list_country = (
        df["manufacturer_country"]
        .value_counts()
        .reset_index(name="count")
        # .head(10)
        .sort_values(by="count", ascending=False)
        .reset_index(drop=True)
    )

    # Get top 10 countries
    top_countries = list_country["manufacturer_country"].tolist()

    # Filter df by top 10 countries
    df = df[df["manufacturer_country"].isin(top_countries)]

    parents = []
    labels = []
    values = []

    for country in df["manufacturer_country"].unique():
        # get the manufacturer country
        df_countries = df[df["manufacturer_country"] == country]

        # add the weapon for the manufacturer country
        for weapon in df_countries["weapon"].unique():
            labels.append(weapon)
            parents.append(country)
            values.append(df_countries[df_countries["weapon"] == weapon].shape[0])

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # Update types
    dict_types = {
        "parent": "string",
        "label": "string",
        "value": "int64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(
    name="Datamarts Sunbusrt Equipments Weapon Type",
    task_run_name="dmt-sunburst-equipments-weapon-type",
)
def dmt_sunbusrt_equipments_weapon_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datamarts Sunburst Equipments Weapon Type
    Count the number of occurrences of each value in the columns of a DataFrame

    Args:
        df: DataFrame

    Returns:
        DataFrame with count aggregations
    """

    # group by country and get top 10
    list_country = (
        df["type_equipment"]
        .value_counts()
        .reset_index(name="count")
        # .head(10)
        .sort_values(by="count", ascending=False)
        .reset_index(drop=True)
    )

    # Get top 10 countries
    top_countries = list_country["type_equipment"].tolist()

    # Filter df by top 10 countries
    df = df[df["type_equipment"].isin(top_countries)]

    parents = []
    labels = []
    values = []

    for country in df["type_equipment"].unique():
        # get the damaged equipment
        df_weap_type = df[df["type_equipment"] == country]

        # add the damaged equipment
        for weap_type in df_weap_type["weapon_type"].unique():
            labels.append(weap_type)
            parents.append(country)
            values.append(
                df_weap_type[df_weap_type["weapon_type"] == weap_type].shape[0]
            )

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # Update types
    dict_types = {
        "parent": "string",
        "label": "string",
        "value": "int64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(
    name="Datamarts Country by Weapon",
    task_run_name="dmt-country-by-weapon",
)
def dmt_country_by_weapon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datamarts Country by Weapon
    Count the number of components by country and weapon
    For a Stacked Bar Chart

    Args:
        df: DataFrame

    Returns:
        DataFrame with count aggregations
    """

    # Retrieve the top 40 weapons
    top_weapons = (
        df["weapon"]
        .value_counts()
        .reset_index(name="count")
        .head(60)
        .sort_values(by="count", ascending=False)
        .reset_index(drop=True)
    )

    # Get top 40 weapons
    top_weapons = top_weapons["weapon"].tolist()

    # Filter df by top 40 weapons
    df = df[df["weapon"].isin(top_weapons)]

    # Order according to the top 40 weapons
    df["weapon"] = pd.Categorical(df["weapon"], categories=top_weapons, ordered=True)
    df = df.sort_values(by="weapon")

    # count number of manufacturer countries by weapon
    df = df.groupby(["weapon", "manufacturer_country"]).size().reset_index(name="count")

    # Remove line count = 0
    df = df[df["count"] != 0]

    # rename columns
    df = df.rename(
        columns={"weapon": "libelle", "manufacturer_country": "metric"}
    ).sort_values(by=["count"], ascending=False)

    # Update types
    dict_types = {
        "libelle": "string",
        "metric": "string",
        "count": "int64",
    }

    # reorder and retype columns
    df = reorder_cols(df, dict_types)
    df = retype_cols(df, dict_types)

    return df


@flow(
    name="DWH Subflow Datamarts Components in Weapons",
    flow_run_name="dwh-subflow-dmt-components-weapons",
    log_prints=True,
)
def subflow_dmt_components_weapons():
    """
    Datamarts Components in Weapons
    """

    # read source compo weapons
    # df_compo_weapons = read_data(PATH_DWH_SILVER_COMPO_WEAPONS, "components_weapons")
    print(f"Reading data from {PATH_DWH_SILVER_COMPO_WEAPONS}")
    df_compo_weapons = read_parquet(PATH_DWH_SILVER_COMPO_WEAPONS, "components_weapons")

    # Global Count
    df_tmp = dmt_global_count(df_compo_weapons)
    rewrite_duckdb_table(
        df_tmp,
        SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS,
        "dmt_global_count",
        PATH_DWH_GOLD,
    )

    # TreeMap Country Weapon Type
    df_tmp = dmt_treemap_country_weapon(df_compo_weapons)
    rewrite_duckdb_table(
        df_tmp,
        SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS,
        "dmt_treemap_country_weapon",
        PATH_DWH_GOLD,
    )

    # Sunburst Equipments Weapon Type
    df_tmp = dmt_sunbusrt_equipments_weapon_type(df_compo_weapons)
    rewrite_duckdb_table(
        df_tmp,
        SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS,
        "dmt_sunburst_equipments_weapon_type",
        PATH_DWH_GOLD,
    )

    # Country by Weapon
    df_tmp = dmt_country_by_weapon(df_compo_weapons)
    rewrite_duckdb_table(
        df_tmp,
        SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS,
        "dmt_country_by_weapon",
        PATH_DWH_GOLD,
    )

    return Completed(message="All DataMarts Components in Weapons Completed")
