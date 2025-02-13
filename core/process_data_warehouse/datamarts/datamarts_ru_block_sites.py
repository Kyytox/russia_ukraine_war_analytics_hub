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

    list_metrics = ["country_domain", "category", "subcategory", "banning_authority"]
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

    daily_counts = (
        df.groupby("date_blocked").size().reset_index(name="sites_blocked_daily")
    )

    monthly_counts = (
        df.groupby("month").size().reset_index(name="sites_blocked_monthly")
    )
    yearly_counts = df.groupby("year").size().reset_index(name="sites_blocked_yearly")

    merged_df = daily_counts
    merged_df = merged_df.merge(
        monthly_counts,
        left_on=merged_df["date_blocked"].dt.to_period("M"),
        right_on="month",
        how="left",
    )
    merged_df = merged_df.merge(
        yearly_counts,
        left_on=merged_df["date_blocked"].dt.year,
        right_on="year",
        how="left",
    )

    # fill nan values
    merged_df = merged_df.fillna(0)

    return merged_df


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
    print("df")
    print(df)

    df["month"] = df["month"].dt.to_period("M")

    # Cont Global
    df_tmp = dmt_global(df)
    path = f"{PATH_DMT_RU_BLOCK_SITES}/count_global.parquet"
    df_tmp.to_parquet(path, index=False)

    # By Date
    df_tmp = dmt_by_date(df)
    path = f"{PATH_DMT_RU_BLOCK_SITES}/count_by_date.parquet"
    df_tmp.to_parquet(path, index=False)

    if df.empty:
        return Failed(message="Data is empty")

    return Completed(message="All DataMarts Russia Blocked Sites have been processed")
