import pandas as pd

from prefect import flow, task
from prefect.blocks.core import schema_extra
from prefect.states import Completed, Failed


# Variables
from core.config.paths import (
    PATH_DWH_SILVER_BLOCKED_SITES,
    PATH_DWH_GOLD,
)

from core.config.variables import (
    SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES,
)

# Utils
from core.libs.duckdb_utils import rewrite_duckdb_table
from core.common.parquet import read_parquet
from core.common.dataframe import reorder_cols, retype_cols


@task(name="dmt-global", task_run_name="dmt-global")
def dmt_global(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by different columns
    """

    print(df.columns)

    # list_metrics = ["country_domain", "category", "subcategory", "banning_authority"]
    list_metrics = ["country_domain", "subcategory", "category", "banning_authority"]
    df_final = pd.DataFrame()

    for metric in list_metrics:
        metric_counts = df.groupby(metric).size().reset_index(name="value")
        metric_counts.rename(columns={metric: "metric"}, inplace=True)
        metric_counts["libelle"] = f"by_{metric}"
        metric_counts["poucentage"] = (
            metric_counts["value"] / metric_counts["value"].sum() * 100
        ).round(2)

        df_final = pd.concat([df_final, metric_counts], ignore_index=True)

    # add total row
    total_row = pd.DataFrame(
        {
            "metric": ["total_blocked_sites"],
            "value": [df.shape[0]],
            "libelle": ["total"],
            "poucentage": [100.0],
        }
    )
    df_final = pd.concat([df_final, total_row], ignore_index=True)

    # add last date row
    last_date_row = pd.DataFrame(
        {
            "metric": pd.to_datetime(df["date_blocked"]).max().strftime("%Y-%m-%d"),
            "value": None,
            "libelle": ["last_date_updated"],
            "poucentage": [None],
        }
    )
    df_final = pd.concat([df_final, last_date_row], ignore_index=True)

    # Update types
    dict_types = {
        "libelle": "string",
        "metric": "string",
        "value": "int64",
        "poucentage": "float64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(name="dmt-by-date", task_run_name="dmt-by-date")
def dmt_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by date.

    Returns four time series:
    - by_day: Daily counts (YYYY-MM-DD)
    - by_month: Monthly counts (YYYY-MM)
    - by_year: Yearly counts (YYYY)
    - by_day_cumul: Cumulative daily counts (YYYY-MM-DD)

    Args:
        df (pd.DataFrame): Source dataframe with a 'date_blocked' column

    Returns:
        pd.DataFrame: Combined datamart with columns: libelle, date, count
    """

    START = pd.Timestamp("2022-01-01")

    # Dates normalisées (sans heure) en datetime64
    dates = pd.to_datetime(df["date_blocked"]).dt.normalize()
    start = min(START, dates.min())
    end = dates.max()

    def build(series: pd.Series, full_index, fmt: str, libelle: str) -> pd.DataFrame:
        """Compte par période, comble les trous avec 0 et formate la date."""
        counts = series.value_counts().reindex(full_index, fill_value=0).sort_index()
        return pd.DataFrame(
            {
                "libelle": libelle,
                "date": counts.index.strftime(fmt),
                "value": counts.to_numpy().astype("int64"),
            }
        )

    # by_day
    df_day = build(
        dates,
        pd.date_range(start, end, freq="D"),
        "%Y-%m-%d",
        "by_day",
    )

    # by_month
    df_month = build(
        dates.dt.to_period("M"),
        pd.period_range(start, end, freq="M"),
        "%Y-%m",
        "by_month",
    )

    # by_year
    df_year = build(
        dates.dt.to_period("Y"),
        pd.period_range(start, end, freq="Y"),
        "%Y",
        "by_year",
    )

    # by_day_cumul
    df_day_cumul = df_day.copy()
    df_day_cumul["value"] = df_day_cumul["value"].cumsum()
    df_day_cumul["libelle"] = "by_day_cumul"

    # Combine all dataframes into one
    df_concat = pd.concat(
        [df_day, df_month, df_year, df_day_cumul],
        ignore_index=True,
    )

    # reorder and retype columns
    dict_types = {
        "libelle": "string",
        "date": "string",
        "value": "int64",
    }

    df_concat = reorder_cols(df_concat, dict_types)
    df_concat = retype_cols(df_concat, dict_types)
    df_concat["date"] = df_concat["date"].astype(str)

    return df_concat


@task(name="dmt-metrics-over-time", task_run_name="dmt-metrics-over-time")
def dmt_metrics_over_time(df: pd.DataFrame, metrics: list[str]) -> pd.DataFrame:
    """
    Create a single datamart for number of blocked sites by month and metrics.

    Returns a long-format table (not pivoted):
    - id: The metric/theme identifier (country_domain, category, ...)
    - month: Month in YYYY-MM format
    - metric: The metric value (country, authority, category, etc.)
    - count: Number of blocked sites

    Data reduction strategy:
    - Only 'relevant' data is kept (count > 0)
    - Missing months are added ONLY from the first appearance
      of each metric value to now (no zero-padding back to 2022)

    Args:
        df (pd.DataFrame): Source dataframe with 'month' column and metric columns
        metrics (list[str]): Metric column names (e.g., ['country_domain', 'category'])

    Returns:
        pd.DataFrame: Long-format dataframe with columns: id, month, metric, count
    """
    # Create full date range
    now = pd.Timestamp.now()
    df_date = pd.DataFrame(
        {"month": pd.date_range(start="2022-01-01", end=now, freq="MS")}
    )

    all_dfs = []

    for metric in metrics:
        # Group by month and metric
        df_tmp = df.groupby(["month", metric]).size().reset_index(name="value")

        # Handle missing values by metric type
        if metric == "banning_authority":
            df_tmp[metric] = df_tmp[metric].fillna("Not specified")
        elif metric == "country_domain":
            df_tmp[metric] = df_tmp[metric].fillna("Unknown")

        # Filter top metrics if needed
        if metric == "country_domain":
            top_countries = (
                df_tmp.groupby("country_domain")["value"]
                .sum()
                .nlargest(25)
                .index.tolist()
            )
            df_tmp = df_tmp[df_tmp["country_domain"].isin(top_countries)]

        # Keep only relevant data (count > 0)
        df_tmp = df_tmp[df_tmp["value"] > 0]

        if df_tmp.empty:
            continue

        # First appearance month for each metric value
        first_months = df_tmp.groupby(metric)["month"].min()

        # Expand each metric value only from its first appearance to now
        df_tmp = df_tmp.merge(
            df_date, on="month", how="right"
        )  # not used directly, kept for clarity
        # -> remove above line, real expansion below

        expanded_parts = []
        for value, first_month in first_months.items():
            months = pd.date_range(start=first_month, end=now, freq="MS")
            df_value = pd.DataFrame({"month": months})
            df_value[metric] = value
            expanded_parts.append(df_value)

        df_expanded = pd.concat(expanded_parts, ignore_index=True)

        # Merge with actual counts
        df_metric_final = df_expanded.merge(df_tmp, on=["month", metric], how="left")

        # Fill missing counts with 0
        df_metric_final["value"] = df_metric_final["value"].fillna(0).astype(int)

        # Rename metric column and add theme identifier
        df_metric_final = df_metric_final.rename(columns={metric: "metric"})
        df_metric_final["id"] = metric

        all_dfs.append(df_metric_final)

    # Concatenate all metrics into a single dataframe
    df_final = pd.concat(all_dfs, ignore_index=True).sort_values(["month"])

    # Format month as string (YYYY-MM)
    df_final["month"] = (
        pd.to_datetime(df_final["month"]).dt.strftime("%Y-%m").astype(str)
    )

    # Remove null metrics
    df_final = df_final[df_final["metric"].notnull()]

    # Type casting
    dict_types = {
        "id": "string",
        "month": "string",
        "metric": "string",
        "value": "int64",
    }

    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(name="dmt-by-authority-category", task_run_name="dmt-by-authority-category")
def dmt_by_authority_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create datamart for number of blocked sites by authority and category

    Returns long-format table with 3 columns:
    - libelle: Banning authority
    - metric: Category
    - count: Number of blocked sites

    Args:
        df (pd.DataFrame): Source dataframe with 'banning_authority' and 'category' columns

    Returns:
        pd.DataFrame: Long-format dataframe with columns: libelle, metric, count
    """
    # Group by authority and category to get counts
    df_tmp = (
        df.groupby(["banning_authority", "category"]).size().reset_index(name="value")
    )

    # Handle missing values
    df_tmp["banning_authority"] = df_tmp["banning_authority"].fillna("Not specified")
    df_tmp["category"] = df_tmp["category"].fillna("Unknown")

    # Rename columns for clarity
    df_final = df_tmp.rename(
        columns={
            "banning_authority": "libelle",
            "category": "metric",
        }
    )

    # Ensure proper order and types
    dict_types = {
        "libelle": "string",
        "metric": "string",
        "value": "int64",
    }

    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

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
    df["country_domain"] = df["country_domain"].fillna("Unknown")

    # Count by country and category
    counts = df.groupby(["country_domain", "category"]).size().reset_index(name="value")

    # Create dataframe for treemap
    df_final = []

    # Add category rows (second level)
    for _, row in counts.iterrows():
        df_final.append(
            {
                "libelle": row["country_domain"],
                "metric": row["category"],
                "value": row["value"],
            }
        )

    dict_types = {
        "libelle": "string",
        "metric": "string",
        "value": "int64",
    }

    df_final = reorder_cols(pd.DataFrame(df_final), dict_types)
    df_final = retype_cols(df_final, dict_types)

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
    df_sorted = df.groupby("country_domain").size().reset_index(name="value")
    list_sorted_countries = df_sorted.sort_values("value", ascending=False)[
        "country_domain"
    ].tolist()

    # Count by country and category
    df = df.groupby(["country_domain", "category"]).size().reset_index(name="value")

    # Sort df according to list TOP_COUNTRY
    df["country_domain"] = pd.Categorical(
        df["country_domain"], categories=list_sorted_countries, ordered=True
    )
    df = df.sort_values("country_domain")

    # Pivot
    df = df.pivot_table(
        index="country_domain",
        columns="category",
        values="value",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    return df


@flow(
    name="DWH Subflow Datamarts Blocked Sites",
    flow_run_name="dwh-subflow-dmt-blocked-sites",
    log_prints=True,
)
def subflow_dmt_blocked_sites():
    """
    Datamarts Russia Blocked Sites
    """

    # read data from transform
    df = read_parquet(PATH_DWH_SILVER_BLOCKED_SITES, "blocked_sites")

    schema = SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES

    # Cont Global
    df_tmp = dmt_global(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_global", PATH_DWH_GOLD)

    # By Date
    df_tmp = dmt_by_date(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_by_date", PATH_DWH_GOLD)

    # All metrics over time (single datamart)
    df_tmp = dmt_metrics_over_time(
        df, ["country_domain", "category", "banning_authority"]
    )
    rewrite_duckdb_table(df_tmp, schema, "dmt_metrics_over_time", PATH_DWH_GOLD)

    # By Authorithy and Category
    df_tmp = dmt_by_authority_category(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_by_authority_category", PATH_DWH_GOLD)

    # Blocked by Country Domain and Category Top 6
    df_tmp = dmt_country_by_category_top6(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_by_country_by_category_top_6", PATH_DWH_GOLD
    )

    # Blocked by Country Domain and Category Top 6 +
    df_tmp = dmt_country_by_category_after6(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_by_country_by_category_after_6", PATH_DWH_GOLD
    )

    # dmt_all_data
    rewrite_duckdb_table(df, schema, "dmt_all_data", PATH_DWH_GOLD)

    if df.empty:
        return Failed(message="Data is empty")

    return Completed(message="All DataMarts Russia Blocked Sites have been processed")
