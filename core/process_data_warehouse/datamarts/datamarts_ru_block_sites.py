import os
import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.states import Completed, Failed


# Variables
from core.config.paths import (
    PATH_DWH_SOURCES,
    PATH_DMT_RU_BLOCK_SITES,
)

# Utils
from core.libs.utils import read_data, save_data


@task(name="dmt-global", task_run_name="dmt-global")
def dmt_global(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by different columns
    """

    # list_metrics = ["country_domain", "category", "subcategory", "banning_authority"]
    list_metrics = ["country_domain", "subcategory", "category", "banning_authority"]
    final_df = pd.DataFrame()

    for metric in list_metrics:
        metric_counts = df.groupby(metric).size().reset_index(name="count")
        metric_counts.rename(columns={metric: "metric"}, inplace=True)
        metric_counts["type_metric"] = f"by_{metric}"
        metric_counts["poucentage"] = (
            metric_counts["count"] / metric_counts["count"].sum() * 100
        ).round(2)

        final_df = pd.concat([final_df, metric_counts], ignore_index=True)

    return final_df


@task(name="dmt-by-date", task_run_name="dmt-by-date")
def dmt_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by date
    - Day
    - Month
    - Year

    ** month and year are already in the dataframe

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with datamart by date
    """

    # By Day
    df_day = (
        df.groupby("date_blocked")
        .size()
        .reset_index(name="count")
        .rename(columns={"date_blocked": "date"})
    )

    # add 1,2,3 january 2022
    df_day = pd.concat(
        [
            df_day,
            pd.DataFrame(
                {
                    "date": pd.date_range(start="2022-01-01", end="2022-01-03"),
                    "count": 0,
                }
            ),
        ],
        ignore_index=True,
    )

    # add absent days
    df_day = df_day.set_index("date").resample("D").asfreq().reset_index()
    df_day["count"] = df_day["count"].fillna(0).astype(int)
    df_day["type_metric"] = "by_day"  # add col type_metrics
    print(df_day)

    # By Month
    df_month = (
        df.groupby("month")
        .size()
        .reset_index(name="count")
        .rename(columns={"month": "date"})
    )

    # add absent months
    # convert to period
    df_month["date"] = df_month["date"].dt.to_period("M")
    df_month = df_month.set_index("date").resample("M").asfreq().reset_index()
    df_month["count"] = df_month["count"].fillna(0).astype(int)
    df_month["date"] = df_month["date"].dt.to_timestamp()
    df_month["type_metric"] = "by_month"  # add col type_metrics

    # By Year
    df_year = (
        df.groupby("year")
        .size()
        .reset_index(name="count")
        .rename(columns={"year": "date"})
    )
    df_year["date"] = pd.to_datetime(df_year["date"], format="%Y")
    df_year["type_metric"] = "by_year"  # add col type_metrics

    # By day cumul
    df_day_cumul = df_day.copy()
    df_day_cumul["count"] = df_day_cumul["count"].cumsum()
    df_day_cumul["type_metric"] = "by_day_cumul"

    # concat
    df_concat = pd.concat(
        [
            df_day,
            df_month,
            df_year,
            df_day_cumul,
        ],
        ignore_index=True,
    )
    return df_concat


@task(name="dmt-by-metrics-over-time", task_run_name="dmt-by-metrics-over-time")
def dmt_by_metrics_over_time(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by month and metrics with pivot

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df_cat (pd.DataFrame): Pivoted dataframe with months as index and categories as columns
    """
    # range date
    df_date = pd.DataFrame(
        {"month": pd.date_range(start="2022-01-01", end="2024-12-31", freq="MS")}
    )

    # Group by month and authority
    df_tmp = df.groupby(["month", metric]).size().reset_index(name="count")

    if metric == "banning_authority":
        # Replace NaN in banning_authority with "Not specified"
        df_tmp[metric] = df_tmp[metric].fillna("Not specified")
    elif metric == "country_domain":
        # Replace None in country_domain with "Unknown"
        df_tmp[metric] = df_tmp[metric].fillna("Unknown")

    # Merge with date range for missing values
    df_tmp = df_date.merge(df_tmp, on="month", how="left")
    df_tmp["count"] = df_tmp["count"].fillna(0).astype(int)

    # Format month as string
    df_tmp["month_str"] = df_tmp["month"].astype(str).str[:7]

    if metric == "country_domain":
        # Get top 25 countries for better visibility
        top_countries = (
            df_tmp.groupby("country_domain")["count"].sum().nlargest(25).index.tolist()
        )
        df_tmp = df_tmp[df_tmp["country_domain"].isin(top_countries)]

    # Pivot the data
    df_final = df_tmp.pivot_table(
        index="month",
        columns=metric,
        values="count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # Format month for display
    df_final["month_str"] = pd.to_datetime(df_final["month"]).dt.strftime("%Y-%m")

    # Reorder columns to put month_str first
    cols = df_final.columns.tolist()
    cols.remove("month_str")
    df_final = df_final[["month_str"] + cols]

    return df_final


@task(name="dmt-by-authority-category", task_run_name="dmt-by-authority-category")
def dmt_by_authority_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by authority and category
    For use in Sankey diagram

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with datamart by authority and category
    """

    # create a df for use Sankey diagram
    df_final = (
        df.groupby(["banning_authority", "category"]).size().reset_index(name="value")
    )

    # Get unique values for nodes
    authorities = df_final["banning_authority"].unique()
    categories = df_final["category"].unique()

    # Create node labels list
    labels = list(authorities) + list(categories)

    # Create source, target and value lists
    source = [list(authorities).index(x) for x in df_final["banning_authority"]]
    target = [
        len(authorities) + list(categories).index(x) for x in df_final["category"]
    ]
    value = df_final["value"].tolist()

    # add missing values to labels
    labels = labels + [""] * (len(source) - len(labels))

    # Create final dataframe
    df_final = pd.DataFrame(
        {"source": source, "target": target, "value": value, "labels": labels}
    )

    return df_final


@task(name="get-list-top-countries", task_run_name="get-list-top-countries")
def get_list_top_countries(df: pd.DataFrame, n: int = 6) -> list:
    """
    Get the top n countries with the most blocked sites

    Args:
        df (pd.DataFrame): Dataframe to control
        n (int): Number of top countries to get

    Returns:
        list: List of top n countries
    """

    # Get the top n countries with the most blocked sites
    top_countries = df["country_domain"].value_counts().head(n).index.tolist()
    return top_countries


@task(name="dmt-country-by-category-top6", task_run_name="dmt-country-by-category-top6")
def dmt_country_by_category_top6(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by country and category
    For use in Treemap diagram

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with datamart by country and category
    """

    # Filter out rows with missing country_domain
    df = df.copy()
    df["country_domain"] = df["country_domain"].fillna("Unknown")

    # Get the top 6 countries with the most blocked sites
    top_countries = get_list_top_countries(df, n=6)

    # Filter dataframe to only include top countries
    df_filtered = df[df["country_domain"].isin(top_countries)]

    # Count by country and category
    counts = (
        df_filtered.groupby(["country_domain", "category"])
        .size()
        .reset_index(name="value")
    )

    # Create dataframe for treemap
    df_final = []

    # Add country rows (first level)
    for country in top_countries:
        country_total = counts[counts["country_domain"] == country]["value"].sum()
        df_final.append(
            {"id": country, "label": country, "parent": "", "value": country_total}
        )

    # Add category rows (second level)
    for _, row in counts.iterrows():
        df_final.append(
            {
                "id": f"{row['country_domain']}-{row['category']}",
                "label": row["category"],
                "parent": row["country_domain"],
                "value": row["value"],
            }
        )

    return pd.DataFrame(df_final)


@task(
    name="dmt-country-by-category-after6",
    task_run_name="dmt-country-by-category-after6",
)
def dmt_country_by_category_after6(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by country and category
    For countrues that are not in the top 6
    For use in Stacked Bar diagram

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with datamart by country and category
    """

    # Get the top 6 countries with the most blocked sites
    top_countries = get_list_top_countries(df, n=6)

    # Filter dataframe to not include top countries
    df = df[~df["country_domain"].isin(top_countries)]

    # get list of countries sorted by count
    df_sorted = df.groupby("country_domain").size().reset_index(name="count")
    list_sorted_countries = df_sorted.sort_values("count", ascending=False)[
        "country_domain"
    ].tolist()

    # Count by country and category
    df = df.groupby(["country_domain", "category"]).size().reset_index(name="count")

    # Sort df according to list TOP_COUNTRY
    df["country_domain"] = pd.Categorical(
        df["country_domain"], categories=list_sorted_countries, ordered=True
    )
    df = df.sort_values("country_domain")

    # Pivot
    df = df.pivot_table(
        index="country_domain",
        columns="category",
        values="count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    return df


@flow(
    name="DWH Subflow Datamarts Russia Blocked Sites",
    flow_run_name="dwh-subflow-dmt-russia-blocked-sites",
    log_prints=True,
)
def flow_dmt_russia_blocked_sites():
    """
    Datamarts Russia Blocked Sites
    """

    # read data from transform
    df = read_data(PATH_DWH_SOURCES, "russia_blocked_sites")

    # Cont Global
    df_tmp = dmt_global(df)
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_global", df_tmp)

    # By Date
    df_tmp = dmt_by_date(df)
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_date", df_tmp)

    # Authority over time
    df_tmp = dmt_by_metrics_over_time(df, "banning_authority")
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_authority_over_time", df_tmp)

    # Country over time
    df_tmp = dmt_by_metrics_over_time(df, "country_domain")
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_country_over_time", df_tmp)

    # Category over time
    df_tmp = dmt_by_metrics_over_time(df, "category")
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_category_over_time", df_tmp)

    # By Authorithy and Category
    df_tmp = dmt_by_authority_category(df)
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_authority_category", df_tmp)

    # Blocked by Country Domain and Category Top 6
    df_tmp = dmt_country_by_category_top6(df)
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_country_by_category_top_6", df_tmp)

    # Blocked by Country Domain and Category Top 6 +
    df_tmp = dmt_country_by_category_after6(df)
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_by_country_by_category_after_6", df_tmp)

    # dmt_all_data
    save_data(PATH_DMT_RU_BLOCK_SITES, "dmt_all_data", df)

    if df.empty:
        return Failed(message="Data is empty")

    return Completed(message="All DataMarts Russia Blocked Sites have been processed")
