import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.states import Completed, Failed


# Variables
from core.config.paths import (
    PATH_DWH_BRONZE_INC_RAILWAY,
    PATH_DWH_GOLD,
)

from core.config.variables import SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY

# Utils
from core.common.parquet import read_parquet
from core.libs.duckdb_utils import rewrite_duckdb_table
from core.common.dataframe import reorder_cols, retype_cols


@task(name="Get all incidents of sabotage")
def sabotage_by_partisans_group(df):
    """
    Get all incidents of sabotage
    """

    df = df[df["inc_type"] == "Sabotage"]
    df = df[["date", "prtsn_grp"]]

    return df


@task(name="Prepare data for a heatmap chart")
def applicable_laws_partisans_age(df):
    """
    Prepare data for a heatmap chart
    For each age unique, count the number of applicable laws
    """

    # Get unique age and names (age != "")
    df_age_name = (
        df.query("prtsn_age != ''")
        .assign(
            prtsn_age=df["prtsn_age"].str.split(","),
            prtsn_names=df["prtsn_names"].str.split(","),
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Create a DataFrame to store the results
    df_final = pd.DataFrame()

    # Count the number of applicable laws
    df_tmp = (
        df_age_name.groupby(["prtsn_age", "app_laws"])
        .size()
        .reset_index(name="total_inc")
    )

    # remove rows "Law not indicated"
    df_tmp = df_tmp.query("app_laws != ''")

    # Pivot table
    df_final = df_tmp.pivot_table(
        index="prtsn_age",
        columns="app_laws",
        values="total_inc",
        fill_value=0,
    )

    return df_final


@task(name="Prepare data for a wordcloud chart")
def wordcloud(df):
    """
    Prepare data for a wordcloud chart
    join all words in specific columns
    """

    list_cols = [
        "region",
        "inc_type",
        "dmg_eqp",
        "prtsn_grp",
        "app_laws",
    ]

    # join all words in specific columns
    df_final = pd.DataFrame()
    df_final["text"] = df[list_cols].apply(lambda x: ", ".join(x.dropna()), axis=1)

    # decompose text (split by ,)
    df_final = (
        df_final["text"]
        .str.split(", ", expand=True)
        .stack()
        .reset_index()
        .drop(columns=["level_0", "level_1"])
    )
    df_final.columns = ["text"]

    # count words
    df_final = df_final["text"].value_counts().reset_index()
    df_final.columns = ["text", "total_inc"]

    return df_final


# ---
#
#
#
#
#
#
#
#
@task(name="DMT KPIs")
def dmt_kpis(df):
    """
    Compute Dmt KPIs:
    - Last date updated
    - Total incidents
    - Total Sabotage
    - Total Arrested Partisans
    """
    if df.empty:
        return pd.DataFrame(columns=["libelle", "metric", "value"])

    # collect all age (values is string with ,)
    df_names = (
        df[["prtsn_age", "prtsn_names", "app_laws"]]
        .dropna()
        .query(
            "prtsn_age != '' and prtsn_names != '' and app_laws != '' and prtsn_age != '0'"
        )
    ).drop_duplicates()

    df_names = (
        df_names.assign(prtsn_names=df["prtsn_names"].str.split(","))
        .explode(["prtsn_names"])
        .reset_index(drop=True)
    )

    last_date = df["date"].max()
    kpis = [
        {
            "libelle": "kpi1",
            "metric": "last_date_updated",
            "value": last_date.strftime("%Y-%m-%d"),
        },
        {
            "libelle": "kpi2",
            "metric": "total_incidents",
            "value": len(df),
        },
        {
            "libelle": "kpi3",
            "metric": "total_sabotage",
            "value": (df["inc_type"] == "Sabotage").sum(),
        },
        {
            "libelle": "kpi4",
            "metric": "total_arrested_partisans",
            "value": df_names["prtsn_names"].count(),
        },
    ]

    return pd.DataFrame(kpis)


@task(name="Overview global incidents")
def overview_global_incidents(df):
    """
    Overview global incidents:
    - Total incidents by year
    - Total incidents by month
    - Total by incident type
    - Total by damaged equipment
    - Total by Region
    - Total by Collisions with
    - Total Sabotage by year
    - Total Sabotage by Partisans group
    - Total Damaged equipment for incident Sabotage
    - Total Sabotage by Region
    - Total Sabotage
    - Total Sabotage with arrested partisans
    - Total Sabotage without arrested partisans
    - Total partisans arrested
    - Total applicable laws by partisans
    - Total partisans age (<18, 18-30, 31-50, >50)
    """
    if df.empty:
        return pd.DataFrame(columns=["metric", "libelle", "value"])

    results = []

    # Year
    year = df.groupby("year").size().reset_index(name="value")
    year.columns = ["metric", "value"]
    year["libelle"] = "year"
    results.append(year)

    # Month (avec format MM-YYYY)
    month = df.groupby(["year", "month"]).size().reset_index(name="value")
    month["metric"] = (
        month["month"].apply(lambda x: f"{x:02d}") + "-" + month["year"].astype(str)
    )
    month["libelle"] = "month"
    results.append(month[["metric", "libelle", "value"]])

    # Incident type
    inc_type = df.groupby("inc_type").size().reset_index(name="value")
    inc_type.columns = ["metric", "value"]
    inc_type["libelle"] = "inc_type"
    results.append(inc_type)

    # Damaged equipment (filtre Human)
    dmg_eqp = (
        df[df["coll_with"] != "Human"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp.columns = ["metric", "value"]
    dmg_eqp["libelle"] = "dmg_eqp"
    results.append(dmg_eqp)

    # Region
    region = df.groupby("region").size().reset_index(name="value")
    region.columns = ["metric", "value"]
    region["libelle"] = "region"
    results.append(region)

    # Collisions with
    coll_with = (
        df[df["inc_type"] == "Collision"]
        .groupby("coll_with")
        .size()
        .reset_index(name="value")
    )
    coll_with.columns = ["metric", "value"]
    coll_with["libelle"] = "coll_with"
    results.append(coll_with)

    # Damaged equipment for incident Collisions
    dmg_eqp_coll = (
        df[df["inc_type"] == "Collision"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp_coll.columns = ["metric", "value"]
    dmg_eqp_coll["libelle"] = "dmg_eqp_coll"
    results.append(dmg_eqp_coll)

    # Sabotage by year
    sabotage_year = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("year")
        .size()
        .reset_index(name="value")
    )
    sabotage_year.columns = ["metric", "value"]
    sabotage_year["libelle"] = "sabotage_year"
    results.append(sabotage_year)

    # Sabotage by Partisans group
    sabotage_prtsn_grp = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("prtsn_grp")
        .size()
        .reset_index(name="value")
    )
    sabotage_prtsn_grp.columns = ["metric", "value"]
    sabotage_prtsn_grp["libelle"] = "prtsn_grp"
    results.append(sabotage_prtsn_grp)

    # Damaged equipment for incident Sabotage
    dmg_eqp_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp_sabotage.columns = ["metric", "value"]
    dmg_eqp_sabotage["libelle"] = "dmg_eqp_sabotage"
    results.append(dmg_eqp_sabotage)

    # Sabotage by Region
    sabotage_region = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("region")
        .size()
        .reset_index(name="value")
    )
    sabotage_region.columns = ["metric", "value"]
    sabotage_region["libelle"] = "region_sabotage"
    results.append(sabotage_region)

    # Total Sabotage
    total_sabotage = pd.DataFrame(
        {
            "metric": ["Total Sabotage"],
            "value": [df[df["inc_type"] == "Sabotage"].shape[0]],
            "libelle": ["total_sabotage"],
        }
    )
    results.append(total_sabotage)

    # Total Sabotage with arrested partisans
    total_sabotage_arrested = pd.DataFrame(
        {
            "metric": ["Sabotage with arrestation"],
            "value": [
                df[(df["inc_type"] == "Sabotage") & (df["prtsn_arr"] == True)].shape[0]
            ],
            "libelle": ["total_sabotage_arrested"],
        }
    )
    results.append(total_sabotage_arrested)

    # Total Sabotage without arrested partisans
    total_sabotage_no_arrested = pd.DataFrame(
        {
            "metric": ["Sabotage without arrestation"],
            "value": [
                df[(df["inc_type"] == "Sabotage") & (df["prtsn_arr"] == False)].shape[0]
            ],
            "libelle": ["total_sabotage_no_arrested"],
        }
    )
    results.append(total_sabotage_no_arrested)

    # Total partisans arrested
    df_names = (
        df[["prtsn_age", "prtsn_names", "app_laws"]]
        .dropna()
        .query(
            "prtsn_age != '' and prtsn_names != '' and app_laws != '' and prtsn_age != '0'"
        )
    ).drop_duplicates()

    df_names = (
        df_names.assign(prtsn_names=df["prtsn_names"].str.split(","))
        .explode(["prtsn_names"])
        .reset_index(drop=True)
    )

    total_partisans_arrested = pd.DataFrame(
        {
            "metric": ["Partisans Arrested"],
            "value": [df_names["prtsn_names"].count()],
            "libelle": ["total_partisans_arrested"],
        }
    )
    results.append(total_partisans_arrested)

    # Total applicable laws by partisans
    total_applicable_laws = (
        df_names.groupby("app_laws").size().reset_index(name="value")
    )
    total_applicable_laws.columns = ["metric", "value"]
    total_applicable_laws["libelle"] = "Laws"
    results.append(total_applicable_laws)

    # Total partisans age (<18, 18-30, 31-50, >50)
    def categorize_age(age):
        if age < 18:
            return "<18"
        elif 18 <= age <= 30:
            return "18-30"
        elif 31 <= age <= 50:
            return "31-50"
        else:
            return ">50"

    df_names["age_category"] = (
        df_names["prtsn_age"].str.split(",").str[0].astype(int).apply(categorize_age)
    )
    total_partisans_age = (
        df_names.groupby("age_category").size().reset_index(name="value")
    )
    total_partisans_age.columns = ["metric", "value"]
    total_partisans_age["libelle"] = "partisans_age"
    results.append(total_partisans_age)

    return pd.concat(results, ignore_index=True)


@task(name="DMT Incidents by month")
def dmt_incidents_by_month(df):
    """
    DMT Incidents by month

    - Total incidents by month and year
    """

    df_tmp = df.groupby(["year", "month"]).size().reset_index(name="total_inc")
    df_tmp["libelle"] = df_tmp["year"].astype(str)
    df_tmp["metric"] = df_tmp["month"].apply(lambda x: f"{x:02d}")
    df_tmp = df_tmp[["libelle", "metric", "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by day")
def dmt_incidents_by_day(df):
    """
    DMT Incidents by day

    - Total incidents by day
    """

    df_tmp = df.groupby("date").size().reset_index(name="total_inc")
    df_tmp["libelle"] = df_tmp["date"].dt.strftime("%Y-%m-%d")
    df_tmp["metric"] = df_tmp["date"].dt.strftime("%Y-%m-%d")
    df_tmp = df_tmp[["libelle", "metric", "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by region")
def dmt_incidents_by_region(df):
    """
    DMT Incidents by region

    - Total incidents by region
    """

    df_tmp = df.groupby("region").size().reset_index(name="total_inc")
    df_tmp["libelle"] = "Regions"
    df_tmp["metric"] = df_tmp["region"]
    df_tmp = df_tmp[["libelle", "metric", "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    # remove rows with empty label
    df_tmp = df_tmp.query("metric != ''")

    return df_tmp


@task(name="DMT Incidents Metric month/year")
def dmt_metric_by_month(df, metric):
    """
    DMT Incidents Metric month/year

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type', 'dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value
    """

    df_tmp = (
        df.groupby(["year", "month", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    ).dt.strftime("%Y-%m")

    # select cols
    df_tmp = df_tmp[["month_year", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by year")
def dmt_metric_by_year(df, metric):
    """
    DMT Incidents by metric and by year

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type', 'dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    df_tmp = (
        df.groupby(["year", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year"])
    )

    # select cols
    df_tmp = df_tmp[["year", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by region")
def dmt_metric_by_region(df, metric):
    """
    DMT Incidents by metric and by region

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type','dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value
    """

    df_tmp = (
        df.groupby(["region", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["region"])
    )

    # remove region with empty label
    df_tmp = df_tmp.query("region != ''")

    # select cols
    df_tmp = df_tmp[["region", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by month/year (cumulative)")
def dmt_metric_by_month_cumul(df, metric):
    """
    DMT Incidents by metric and by month/year (cumulative)

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type','dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    df_tmp = (
        df.groupby(["year", "month", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    )

    # compute cumulative sum by incident type
    df_tmp["cumul_inc"] = df_tmp.groupby(metric)["total_inc"].cumsum()

    # Create a complete date range from min to max month
    date_min = df_tmp["month_year"].min()
    date_max = df_tmp["month_year"].max()
    date_range = pd.date_range(start=date_min, end=date_max, freq="MS")

    # Get unique incident types
    incident_types = df_tmp[metric].unique()

    # Create a complete grid: all incident types × all months
    complete_grid = pd.MultiIndex.from_product(
        [incident_types, date_range], names=[metric, "month_year"]
    ).to_frame(index=False)

    # Merge with existing data (right join to keep all months)
    df_tmp = df_tmp[[metric, "month_year", "cumul_inc"]].merge(
        complete_grid, on=[metric, "month_year"], how="right"
    )

    # Sort by incident type and date
    df_tmp = df_tmp.sort_values([metric, "month_year"])

    # Forward fill missing values by incident type (fill with last known cumulative)
    df_tmp["cumul_inc"] = df_tmp.groupby(metric)["cumul_inc"].ffill()

    # select cols
    df_tmp = df_tmp[[metric, "month_year", "cumul_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by incident type and by damaged equipment")
def dmt_incidents_on_2_lvl(df, lvl_1, lvl_2):
    """
    DMT Incidents on 2 levels:
        example: incident type and damaged equipment

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        lvl_1 (str): The first level column name (e.g., 'inc_type').
        lvl_2 (str): The second level column name (e.g., 'dmg_eqp').

    Returns:
        pd.DataFrame: DataFrame with columns 'parent', 'label', and 'value'
    """

    parents = []
    labels = []
    values = []

    for x in df[lvl_1].unique():
        # get the damaged equipment
        df_tmp = df[df[lvl_1] == x]

        # add the damaged equipment
        for y in df_tmp[lvl_2].unique():
            labels.append(y)
            parents.append(x)
            values.append(df_tmp[df_tmp[lvl_2] == y].shape[0])

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "libelle": parents,
            "metric": labels,
            "value": values,
        }
    )

    # Update types
    dict_types = {
        "libelle": "string",
        "metric": "string",
        "value": "int64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(name="DMT Incidents Total vs Sabotage over time")
def dmt_incidents_total_vs_sabotage(df):
    """
    DMT Incidents Total vs Sabotage over time (month/year)

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Total incidents by month/year
    df_total = (
        df.groupby(["year", "month"])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )
    df_total["month_year"] = pd.to_datetime(
        df_total["year"].astype(str) + "-" + df_total["month"].astype(str)
    ).dt.strftime("%Y-%m")
    df_total = df_total[["month_year", "total_inc"]]
    df_total.columns = ["libelle", "value"]
    df_total["metric"] = "Incidents"

    # Sabotage incidents by month/year
    df_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby(["year", "month"])
        .size()
        .reset_index(name="sabotage_inc")
        .sort_values(["year", "month"])
    )
    df_sabotage["month_year"] = pd.to_datetime(
        df_sabotage["year"].astype(str) + "-" + df_sabotage["month"].astype(str)
    ).dt.strftime("%Y-%m")
    df_sabotage = df_sabotage[["month_year", "sabotage_inc"]]
    df_sabotage.columns = ["libelle", "value"]
    df_sabotage["metric"] = "Sabotage"

    # add missing months for sabotage (if no sabotage in a month, value = 0)
    df_sabotage = (
        df_total[["libelle"]]
        .merge(df_sabotage, on="libelle", how="left")
        .fillna({"value": 0, "metric": "Sabotage"})
    ).sort_values("libelle")

    # Merge total and sabotage
    df_final = pd.concat([df_sabotage, df_total], ignore_index=True)

    return df_final


@task(name="DMT Incidents Sabotage by Partisans group over time")
def dmt_incidents_sabotage_by_prtsn_grp_over_time(df):
    """
    DMT Incidents Sabotage by Partisans group over time (day)

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Sabotage incidents by day and partisans group
    df_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby(["date", "prtsn_grp"])
        .size()
        .reset_index(name="sabotage_inc")
        .sort_values(["date", "prtsn_grp"])
    )
    df_sabotage["libelle"] = df_sabotage["date"].dt.strftime("%Y-%m-%d")
    df_sabotage = df_sabotage[["libelle", "prtsn_grp", "sabotage_inc"]]
    df_sabotage.columns = ["libelle", "metric", "value"]

    return df_sabotage


@task(name="DMT Partisans Arrested by Age")
def dmt_partisans_arrested_by_age(df):
    """
    DMT Partisans Arrested by Age

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Get unique age and names (age != "")
    df_age_name = (
        df.query("prtsn_age != ''")
        .assign(
            prtsn_age=df["prtsn_age"].str.split(","),
            prtsn_names=df["prtsn_names"].str.split(","),
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Count the number of applicable laws
    df_tmp = (
        df_age_name.groupby("prtsn_age")
        .size()
        .reset_index(name="total_inc")
        .sort_values("prtsn_age")
    )

    # select cols
    df_tmp = df_tmp[["prtsn_age", "total_inc"]]
    df_tmp.columns = ["libelle", "value"]
    df_tmp["metric"] = "Partisans Arrested by Age"

    return df_tmp


@task(name="DMT Partisans Arrested by Group Age")
def dmt_partisans_arrested_by_group_age(df):
    """
    DMT Partisans Arrested by Group Age

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Get unique age and names (age != "")
    df_age_name = (
        df.query("prtsn_age != ''")
        .assign(
            prtsn_age=df["prtsn_age"].str.split(","),
            prtsn_names=df["prtsn_names"].str.split(","),
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Count the number of applicable laws
    df_tmp = (
        df_age_name.groupby("prtsn_age")
        .size()
        .reset_index(name="total_inc")
        .sort_values("prtsn_age")
    )

    # select cols
    df_tmp = df_tmp[["prtsn_age", "total_inc"]]
    df_tmp.columns = ["libelle", "value"]

    # add age group column
    def age_group(age):
        if age < 18:
            return "<18"
        elif 18 <= age <= 30:
            return "18-30"
        elif 31 <= age <= 50:
            return "31-50"
        else:
            return ">50"

    df_tmp["age_group"] = df_tmp["libelle"].astype(int).apply(age_group)

    # metric is the age group (<18, 18-30, 31-50, >50)
    df_tmp["metric"] = df_tmp["age_group"]

    # select final columns
    df_tmp = df_tmp[["libelle", "metric", "value"]]

    return df_tmp


@task(name="DMT Partisans Arrested by Applicable Laws")
def dmt_partisans_arrested_by_applicable_laws(df):
    """
    DMT Partisans Arrested by Applicable Laws

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """
    # Get unique age and names (age != "")
    df_age_name = (
        df.query("prtsn_age != ''")
        .assign(
            prtsn_age=df["prtsn_age"].str.split(","),
            prtsn_names=df["prtsn_names"].str.split(","),
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Create a DataFrame to store the results
    df_tmp = pd.DataFrame()

    # Count the number of applicable laws
    df_tmp = (
        df_age_name.groupby(["prtsn_age", "app_laws"])
        .size()
        .reset_index(name="total_inc")
    )

    # remove rows "Law not indicated"
    df_tmp = df_tmp.query("app_laws != ''")

    # rename cols
    df_tmp.columns = ["metric", "libelle", "value"]

    return df_tmp


@flow(
    name="DWH Subflow Datamarts Incidents Railway",
    flow_run_name="dwh-subflow-dmt-incident-railway",
    log_prints=True,
)
def subflow_dmt_incident_railway():
    """
    Datamarts Incidents Railway
    """

    # read data from transform
    # df = read_parquet(PATH_DWH_SILVER_INC_RAILWAY, "incidents_railway")
    df = read_parquet(PATH_DWH_BRONZE_INC_RAILWAY, "incidents_railway")

    schema = SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY

    if df.empty:
        return Failed(message="Data is empty")

    # KPIs
    df_tmp = dmt_kpis(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_kpis", PATH_DWH_GOLD)

    # Incidents total
    df_tmp = overview_global_incidents(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_global", PATH_DWH_GOLD)

    # Incidents by month
    df_tmp = dmt_incidents_by_month(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_month", PATH_DWH_GOLD)

    # Incidents by day
    df_tmp = dmt_incidents_by_day(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_day", PATH_DWH_GOLD)

    # Incidents by region
    df_tmp = dmt_incidents_by_region(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_region", PATH_DWH_GOLD)

    #
    #
    # Incidents Types
    # Incidents by incident type and by month/year
    df_tmp = dmt_metric_by_month(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_month", PATH_DWH_GOLD)

    # Incidents by incident type and by year
    df_tmp = dmt_metric_by_year(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_year", PATH_DWH_GOLD)

    # Incidents by incident type and by month/year (cumulative)
    df_tmp = dmt_metric_by_month_cumul(df, "inc_type")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_types_by_month_cumul", PATH_DWH_GOLD
    )

    # Incidents by incident type and by damaged equipment
    df_tmp = dmt_incidents_on_2_lvl(df, "inc_type", "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_types_by_damaged_equipment", PATH_DWH_GOLD
    )

    # Incidents by incident type and by region
    df_tmp = dmt_metric_by_region(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_region", PATH_DWH_GOLD)

    #
    #
    #
    # Damaged Equipment
    # DMT Incidents by damaged equipment and by month/year
    df_tmp = dmt_metric_by_month(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_month", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by year
    df_tmp = dmt_metric_by_year(df, "dmg_eqp")
    rewrite_duckdb_table(df_tmp, schema, "dmt_damaged_equipment_by_year", PATH_DWH_GOLD)

    # DMT Incidents by damaged equipment and by month/year (cumulative)
    df_tmp = dmt_metric_by_month_cumul(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_month_cumul", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by region
    df_tmp = dmt_metric_by_region(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_region", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by incident type
    df_tmp = dmt_incidents_on_2_lvl(df, "dmg_eqp", "inc_type")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_incident_type", PATH_DWH_GOLD
    )

    #
    #
    #
    # Collisions
    # DMT Incidents Type = Collisions by month/year
    # df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "coll_with")
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "coll_with")
    rewrite_duckdb_table(df_tmp, schema, "dmt_collisions_by_month", PATH_DWH_GOLD)

    # DMT Incidents Type = Collisions by damaged equipment and by month/year
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp,
        schema,
        "dmt_collisions_by_damaged_equipment_by_month",
        PATH_DWH_GOLD,
    )

    #
    #
    # Sabotage
    # Sabotages by month/year
    df_tmp = dmt_incidents_by_month(df.query("inc_type == 'Sabotage'"))
    rewrite_duckdb_table(df_tmp, schema, "dmt_sabotage_by_month", PATH_DWH_GOLD)

    # Incidents Total vs Sabotage over time (month/year)
    df_tmp = dmt_incidents_total_vs_sabotage(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_total_vs_sabotage", PATH_DWH_GOLD
    )

    # Sabotage by Partisans group over time (day)
    df_tmp = dmt_incidents_sabotage_by_prtsn_grp_over_time(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_prtsn_grp_over_time", PATH_DWH_GOLD
    )

    # Sabotage by Partisans group and by damaged equipment
    df_tmp = dmt_incidents_on_2_lvl(
        df.query("inc_type == 'Sabotage'"), "prtsn_grp", "dmg_eqp"
    )
    print(df_tmp)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_prtsn_grp_by_damaged_equipment", PATH_DWH_GOLD
    )

    # Sabotage by damaged equipment and by Partisans group
    df_tmp = dmt_incidents_on_2_lvl(
        df.query("inc_type == 'Sabotage'"), "dmg_eqp", "prtsn_grp"
    )
    print(df_tmp)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_damaged_equipment_by_prtsn_grp", PATH_DWH_GOLD
    )

    # Damaged equipment in Sabotage by month/year
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Sabotage'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_damaged_equipment_by_month", PATH_DWH_GOLD
    )

    # Sabotage by Region and by damaged equipment
    df_tmp = dmt_metric_by_region(df.query("inc_type == 'Sabotage'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_region_by_damaged_equipment", PATH_DWH_GOLD
    )

    #
    #
    #
    # Partisans Arrested
    # Partisans Arrested by Age
    df_tmp = dmt_partisans_arrested_by_age(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_partisans_arrested_by_age", PATH_DWH_GOLD)

    # Partisans Arrested by Group Age
    df_tmp = dmt_partisans_arrested_by_group_age(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_partisans_arrested_by_group_age", PATH_DWH_GOLD
    )

    # Partisans Arrested by Applicable Laws
    df_tmp = dmt_partisans_arrested_by_applicable_laws(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_partisans_arrested_by_applicable_laws", PATH_DWH_GOLD
    )

    return Completed(message="All DataMarts Incidents Railway Completed")
