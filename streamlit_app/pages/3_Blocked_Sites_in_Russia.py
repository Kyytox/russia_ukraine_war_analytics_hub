from bs4 import BeautifulSoup
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import shutil
import pathlib
import altair as alt

import streamlit_shadcn_ui as ui
from variables import colors


path = "data/data_warehouse/datamarts/russia_block_sites"


# data / data_warehouse / datamarts / russia_block_sites / count_by_authority_category.parquet
# data / data_warehouse / datamarts / russia_block_sites / count_by_category_over_time.parquet
# data / data_warehouse / datamarts / russia_block_sites / count_by_date_and_metrics.parquet
# data / data_warehouse / datamarts / russia_block_sites / count_by_date.parquet
# data / data_warehouse / datamarts / russia_block_sites / count_global.parquet


dmt_global = pd.read_parquet(f"{path}/count_global.parquet")
dmt_by_date = pd.read_parquet(f"{path}/count_by_date.parquet")
dmt_by_date_and_metrics = pd.read_parquet(f"{path}/count_by_date_and_metrics.parquet")
dmt_by_authority_category = pd.read_parquet(
    f"{path}/count_by_authority_category.parquet"
)
dmt_by_category_over_time = pd.read_parquet(
    f"{path}/count_by_category_over_time.parquet"
)


#
# 1. Charts Dmt Global
st.dataframe(dmt_global.dtypes)

# schema of dmt_global
# metric	object
# count	int64
# type_metric	object
# poucentage	float64

LIST_TOP_30 = (
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]
    .sort_values(by="count", ascending=False)
    .head(30)
    .metric.tolist()
)
# add Not specified
LIST_TOP_30.append("Not specified")
st.write(LIST_TOP_30)


for type_metric in dmt_global["type_metric"].unique():
    col1, col2 = st.columns([0.5, 0.5])
    df = (
        dmt_global[dmt_global["type_metric"] == type_metric]
        .sort_values(by="count", ascending=False)
        .head(30)
    )

    # Charts bar
    bar = (
        alt.Chart(df)
        .mark_bar(
            cornerRadius=4,
            orient="horizontal",
        )
        .encode(
            y=alt.Y("metric", sort="-x"),
            x="count",
            color=alt.Color("metric", legend=None, scale=alt.Scale(scheme="inferno")),
            tooltip=["metric", "count"],
        )
        .properties(
            title=alt.TitleParams(
                f"Blocked Sites {type_metric.title().replace('_', ' ')}",
                anchor="middle",
                subtitle="Top 30",
                subtitleColor="grey",
            ),
        )
    )
    text = bar.mark_text(align="left", baseline="middle", dx=3).encode(
        text="count", color=alt.value("white")
    )

    with col1:
        st.altair_chart(bar + text, use_container_width=True)

    with col2:
        st.write("")
        with st.expander("Data"):
            st.dataframe(
                df[["metric", "count"]].rename(
                    columns={
                        "metric": type_metric.title()
                        .replace("_", " ")
                        .replace("By", "")
                    }
                ),
                hide_index=True,
                width=300,
            )


st.divider()
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#


st.dataframe(dmt_by_date)
st.dataframe(dmt_by_date.dtypes)

# Schema
# date	datetime64[ns]
# count	int64
# type_metric	object


col1, col2 = st.columns([0.5, 0.5])


# By Year
# Bar
df = dmt_by_date[dmt_by_date["type_metric"] == "by_year"]
df["date"] = df["date"].astype(str).str[:4]
year = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        alt.X("date", title="Year", axis=alt.Axis(labelAngle=0)),
        alt.Y("count", title="Count"),
        color=alt.Color("type_metric", legend=None),
        tooltip=["date", "count"],
    )
    .properties(
        title="Blocked Sites by Year",
    )
)

col1.altair_chart(year, use_container_width=True)


# By Month
df = dmt_by_date[dmt_by_date["type_metric"] == "by_month"]
df["date"] = df["date"].astype(str).str[:7]
month = (
    # alt.Chart(dmt_by_date[dmt_by_date["type_metric"] == "by_month"])
    alt.Chart(df)
    .mark_bar(
        cornerRadius=4,
    )
    .encode(
        alt.X("date", title="Month"),
        alt.Y("count", title="Count"),
        color=alt.Color("type_metric", legend=None),
        tooltip=["date", "count"],
    )
    .properties(
        title="Blocked Sites by Month",
    )
)

col2.altair_chart(month, use_container_width=True)


# Day (line , rect )
day_line = (
    alt.Chart(dmt_by_date[dmt_by_date["type_metric"] == "by_day"])
    .mark_line()
    .encode(
        alt.X("date", title="Day", axis=alt.Axis(labelAngle=0)),
        alt.Y("count", title="Count"),
        color=alt.Color("type_metric", legend=None),
        tooltip=["date", "count"],
    )
    .properties(
        title="Blocked Sites by Day",
        height=470,
    )
)

col1.altair_chart(day_line, use_container_width=True)


# Bt Day cuml
df_day_cumul = dmt_by_date[dmt_by_date["type_metric"] == "by_day_cumul"]
day_cumul = (
    alt.Chart(df_day_cumul)
    .mark_line()
    .encode(
        x="date",
        y="count",
        color=alt.Color("type_metric", legend=None),
        tooltip=["date", "count"],
    )
    .properties(
        title="Blocked Sites by Day Cumulative",
        height=470,
    )
)

col2.altair_chart(day_cumul, use_container_width=True)


# for each year
for year in dmt_by_date["date"].dt.year.unique():
    year = str(year)
    df = dmt_by_date[
        (dmt_by_date["type_metric"] == "by_day")
        & (dmt_by_date["date"].dt.year == int(year))
    ]

    day_rect = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            alt.X(
                "date(date):O",
                title="Day",
                axis=alt.Axis(format="%e", labelAngle=0),
            ),
            alt.Y("month(date):O", title="Month"),
            alt.Color("count:Q", title="Count", scale=alt.Scale(scheme="cividis")),
            tooltip=[
                alt.Tooltip("date", title="Date"),
                alt.Tooltip("count", title="Count"),
            ],
        )
        .properties(
            title=f"Blocked Sites by Day in {year}",
        )
    ).configure_axis(domain=False)

    if year == "2022" or year == "2024":
        col1.altair_chart(day_rect, use_container_width=True)
    else:
        col2.altair_chart(day_rect, use_container_width=True)


st.divider()

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#


st.divider()
st.dataframe(dmt_by_date_and_metrics)
st.dataframe(dmt_by_date_and_metrics.dtypes)

# Schema
# month	period[M]
# metric	object
# count	int64
# type_metric	object


col1, col2 = st.columns([0.5, 0.5])

for type_metric in dmt_by_date_and_metrics["type_metric"].unique():
    df = dmt_by_date_and_metrics[dmt_by_date_and_metrics["type_metric"] == type_metric]
    df["month"] = df["month"].astype(str).str[:7]

    st.write(LIST_TOP_30)
    if type_metric == "by_country_domain":
        df = df[df["metric"].isin(LIST_TOP_30)]

    # Bar
    bar = (
        alt.Chart(df)
        .mark_bar(
            cornerRadius=4,
        )
        .encode(
            x=alt.X("month", title="Month"),
            y="count",
            color=alt.Color(
                "metric",
                scale=alt.Scale(scheme="inferno"),
                legend=alt.Legend(
                    orient="right",
                    title=None,
                    labelLimit=100,
                    values=df["metric"].value_counts().nlargest(5).index.tolist(),
                ),
            ),
            tooltip=["metric", "count"],
        )
        .properties(
            title=f"Blocked Sites by Month {type_metric.title().replace('_', ' ')}",
            height=500,
        )
    )

    col1.altair_chart(bar, use_container_width=True)

    # Table
    with col2:
        st.write("")
        with st.expander("Data"):
            st.dataframe(
                df[["month", "metric", "count"]].rename(
                    columns={
                        "metric": type_metric.title()
                        .replace("_", " ")
                        .replace("By", "")
                    }
                ),
                hide_index=True,
                # width=300,
            )

#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#

# st.dataframe(dmt_by_authority_category)
# st.dataframe(dmt_by_category_over_time)
