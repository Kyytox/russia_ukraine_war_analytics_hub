import pandas as pd

from prefect import flow, task
from prefect.states import Completed


# Variables
from core.config.paths import (
    PATH_DWH_SOURCES,
    PATH_DMT_COMPO_WEAPONS,
)

# Utils
from core.libs.utils import read_data, save_data


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

    return df_final


@task(
    name="Datamarts TreeMap Country Weapon Type",
    task_run_name="dmt-treemap-country-weapon-type",
)
def dmt_treemap_country_weapon_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Datamarts TreeMap Country Weapon Type
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
        .head(10)
        .sort_values(by="count", ascending=False)
        .reset_index(drop=True)
    )
    print(list_country)

    # Get top 10 countries
    top_countries = list_country["manufacturer_country"].tolist()
    print(top_countries)

    # Filter df by top 10 countries
    df = df[df["manufacturer_country"].isin(top_countries)]

    ids = []
    parents = []
    labels = []
    values = []

    for country in df["manufacturer_country"].unique():
        ids.append(country)
        labels.append(country)
        parents.append("")
        print('df[df["manufacturer_country"] == country]')
        print(df[df["manufacturer_country"] == country].shape[0])
        values.append(df[df["manufacturer_country"] == country].shape[0])

        # get the damaged equipment
        df_weap_type = df[df["manufacturer_country"] == country]

        # add the damaged equipment
        for weap_type in df_weap_type["weapon_type"].unique():
            weap_type_id = f"{country}_{weap_type}"
            ids.append(weap_type_id)
            labels.append(weap_type)
            parents.append(country)
            values.append(
                df_weap_type[df_weap_type["weapon_type"] == weap_type].shape[0]
            )

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "tab": "treemap",
            "chart": "treemap",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

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
        .head(10)
        .sort_values(by="count", ascending=False)
        .reset_index(drop=True)
    )
    print(list_country)

    # Get top 10 countries
    top_countries = list_country["type_equipment"].tolist()
    print(top_countries)

    # Filter df by top 10 countries
    df = df[df["type_equipment"].isin(top_countries)]

    ids = []
    parents = []
    labels = []
    values = []

    for country in df["type_equipment"].unique():
        ids.append(country)
        labels.append(country)
        parents.append("")
        print('df[df["type_equipment"] == country]')
        print(df[df["type_equipment"] == country].shape[0])
        values.append(df[df["type_equipment"] == country].shape[0])

        # get the damaged equipment
        df_weap_type = df[df["type_equipment"] == country]

        # add the damaged equipment
        for weap_type in df_weap_type["weapon_type"].unique():
            weap_type_id = f"{country}_{weap_type}"
            ids.append(weap_type_id)
            labels.append(weap_type)
            parents.append(country)
            values.append(
                df_weap_type[df_weap_type["weapon_type"] == weap_type].shape[0]
            )

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "tab": "treemap",
            "chart": "treemap",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

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
    print(top_weapons)

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
    print(df)

    return df


@flow(
    name="DWH Subflow Datamarts Components in Weapons",
    flow_run_name="dwh-subflow-dmt-compo-weapons",
    log_prints=True,
)
def flow_dmt_compo_weapons():
    """
    Datamarts Components in Weapons
    """

    # read source compo weapons
    df_compo_weapons = read_data(PATH_DWH_SOURCES, "components_weapons")

    # Global Count
    df_tmp = dmt_global_count(df_compo_weapons)
    save_data(PATH_DMT_COMPO_WEAPONS, "dmt_global_count", df_tmp)

    # TreeMap Country Weapon Type
    df_tmp = dmt_treemap_country_weapon_type(df_compo_weapons)
    save_data(PATH_DMT_COMPO_WEAPONS, "dmt_treemap_country_weapon_type", df_tmp)

    # Sunburst Equipments Weapon Type
    df_tmp = dmt_sunbusrt_equipments_weapon_type(df_compo_weapons)
    save_data(PATH_DMT_COMPO_WEAPONS, "dmt_sunburst_equipments_weapon_type", df_tmp)

    # Country by Weapon
    df_tmp = dmt_country_by_weapon(df_compo_weapons)
    save_data(PATH_DMT_COMPO_WEAPONS, "dmt_country_by_weapon", df_tmp)

    return Completed(message="All DataMarts Components in Weapons Completed")
