from bs4 import BeautifulSoup
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import shutil
import pathlib
import altair as alt
import plotly.graph_objects as go

import streamlit_shadcn_ui as ui


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

dmt_by_country_category = pd.read_parquet(
    f"{path}/count_by_country_domain_category.parquet"
)


def jump_lines(z=0, n=6):
    while z < n:
        st.write("")
        z += 1


###########################################################################
###########################################################################
###########################################################################
###########################################################################
###########################################################################
###########################################################################


# Create sidebar filters
st.sidebar.header("Filters")

# Country domain filter
country_domains = dmt_by_country_category["country_domain"].unique()
country_domains = country_domains.tolist()
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=country_domains,
    placeholder="All",
)

# Category filter from dmt_global
categories = dmt_global[dmt_global["type_metric"] == "by_category"]["metric"].unique()
categories = categories.tolist()
selected_categories = st.sidebar.multiselect(
    "Select Categories",
    options=categories,
    placeholder="All",
)

# Subcategory filter from dmt_global
subcategories = dmt_global[dmt_global["type_metric"] == "by_subcategory"][
    "metric"
].unique()
subcategories = subcategories.tolist()
selected_subcategories = st.sidebar.multiselect(
    "Select Subcategories",
    options=subcategories,
    placeholder="All",
)

# Apply filters to relevant dataframes
if len(selected_countries) > 0:
    dmt_global = dmt_global[
        (dmt_global["type_metric"] != "by_country_domain")
        | (dmt_global["metric"].isin(selected_countries))
    ]
    dmt_by_country_category = dmt_by_country_category[
        dmt_by_country_category["country_domain"].isin(selected_countries)
    ]

if len(selected_categories) > 0:
    dmt_global = dmt_global[
        (dmt_global["type_metric"] != "by_category")
        | (dmt_global["metric"].isin(selected_categories))
    ]
    dmt_by_country_category = dmt_by_country_category[
        dmt_by_country_category["category"].isin(selected_categories)
    ]

if len(selected_subcategories) > 0:
    dmt_global = dmt_global[
        (dmt_global["type_metric"] != "by_subcategory")
        | (dmt_global["metric"].isin(selected_subcategories))
    ]


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


col1, col2 = st.columns([0.5, 0.5])
curr_col = col2
for type_metric in dmt_global["type_metric"].unique():

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

    curr_col = col1 if curr_col == col2 else col2

    with curr_col:
        st.altair_chart(bar + text, use_container_width=True)

        with st.expander("Explore Data - More Explanation", icon="🔍"):
            subcol1, subcol2 = st.columns([0.4, 0.6])
            with subcol1:
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

            with subcol2:
                if type_metric == "by_subcategory":
                    # resume what is miroor subcategory
                    st.write(
                        """'Mirror' subcategory refers to alternative domain names created to bypass Russian censorship.
                        These are primarily used by news services like Radio Liberty/Radio Free Europe (RL/RFE) to maintain 
                        access when their main domains are blocked. When Roskomnadzor blocks one mirror domain, new ones 
                        are quickly created and shared through social media."""
                    )

        jump_lines()


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
                    values=df["metric"].value_counts().nlargest(20).index.tolist(),
                ),
            ),
            tooltip=["metric", "count"],
        )
        .properties(
            title=f"Blocked Sites by Month {type_metric.title().replace('_', ' ')}",
            height=700,
        )
    )

    if type_metric == "by_country_domain":
        col1.altair_chart(bar, use_container_width=True)
    else:
        col2.altair_chart(bar, use_container_width=True)


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
st.dataframe(dmt_by_authority_category)
st.dataframe(dmt_by_authority_category.dtypes)

# Schema dmt_by_authority_category
# source	int64
# target	int64
# value	int64
# labels	object


# Sankey
data = dict(
    type="sankey",
    node=dict(
        pad=21,
        thickness=15,
        line=dict(color="black", width=0.6),
        label=dmt_by_authority_category["labels"],
        color=dmt_by_authority_category["labels"].apply(
            lambda x: "lightblue" if "by" in x else "lightgreen"
        ),
    ),
    link=dict(
        source=dmt_by_authority_category["source"],
        target=dmt_by_authority_category["target"],
        value=dmt_by_authority_category["value"],
        color=dmt_by_authority_category["source"].apply(
            lambda x: "orange" if x == 0 else "blue"
        ),
    ),
)

fig = go.Figure(
    data,
    layout=dict(
        title=dict(
            text="Blocked Sites by Authority and Category",
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text="Sankey Diagram",
            ),
        ),
        font_size=16,
        height=680,
    ),
)
st.plotly_chart(fig, use_container_width=True)

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
st.dataframe(dmt_by_country_category)
st.dataframe(dmt_by_country_category.dtypes)

# Schema dmt_by_country_category
# country_domain	object
# category	object
# count	int64

# in LIST_TOP_30
# LIST_TOP_30_NO_UNKNOWN = [x for x in LIST_TOP_30 if x != "Unknown"]
df = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_30[:6])
]


# Charts Tree Map (altair rect
base = (
    alt.Chart(df)
    .transform_aggregate(count_="sum(count)", groupby=["country_domain", "category"])
    .transform_stack(
        stack="count_",
        as_=["stack_count_Domain1", "stack_count_Domain2"],
        offset="normalize",
        sort=[alt.SortField("country_domain", "ascending")],
        groupby=[],
    )
    .transform_window(
        x="min(stack_count_Domain1)",
        x2="max(stack_count_Domain2)",
        rank_Category="dense_rank()",
        distinct_Category="distinct(category)",
        groupby=["country_domain"],
        frame=[None, None],
        sort=[alt.SortField("category", "ascending")],
    )
    .transform_window(
        rank_Domain="dense_rank()",
        frame=[None, None],
        sort=[alt.SortField("country_domain", "ascending")],
    )
    .transform_stack(
        stack="count_",
        groupby=["country_domain"],
        as_=["y", "y2"],
        offset="normalize",
        sort=[alt.SortField("category", "ascending")],
    )
    .transform_calculate(
        ny="datum.y + (datum.rank_Category) * datum.distinct_Category * 0.01 / 4",
        ny2="datum.y2 + (datum.rank_Category) * datum.distinct_Category * 0.01 / 4",
        nx="datum.x + (datum.rank_Domain) * 0.01",
        nx2="datum.x2 + (datum.rank_Domain) * 0.01",
        xc="(datum.nx+datum.nx2)/2",
        yc="(datum.ny+datum.ny2)/2",
    )
)

# Rectangles pour les catégories
rect = (
    base.mark_rect()
    .encode(
        x=alt.X("nx:Q").axis(None),
        x2="nx2",
        y=alt.Y("ny:Q").axis(None),
        y2="ny2",
        color=alt.Color(
            "country_domain:N", scale=alt.Scale(scheme="category20"), legend=None
        ),
        opacity=alt.Opacity("country_domain:N", legend=None),
        tooltip=[
            alt.Tooltip("country_domain:N", title="Country"),
            alt.Tooltip("category:N", title="Category"),
            alt.Tooltip("count_:Q", title="Count", format=","),
        ],
    )
    .properties(height=700)
)

# Texte pour les catégories
text = base.mark_text(baseline="middle", fontSize=11).encode(
    x=alt.X("xc:Q").axis(None),
    y=alt.Y("yc:Q").title(None),
    text="category:N",
    color=alt.value("white"),
)

# Labels pour les pays
country_labels = base.mark_text(baseline="middle", align="center", fontSize=12).encode(
    x=alt.X("min(xc):Q").title("Country Domain").axis(orient="top"),
    color=alt.Color("country_domain:N", legend=None),
    text="country_domain:N",
)

# Assemblage final
final_chart = (
    (country_labels & (rect + text))
    .resolve_scale(x="shared")
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Country Domain and Category",
            subtitle="For the Top 6 Country Domain",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor="middle",
        ),
    )
    .configure_view(stroke=None)
    .configure_axis(domain=False, ticks=False, labels=False, grid=False)
)

st.altair_chart(final_chart, use_container_width=True)


# Charts bars stacked
df = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_30[6:])
]

bar = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("country_domain", title="Country Domain", sort="-y"),
        y=alt.Y("count", title="Count"),
        color=alt.Color("category"),
        tooltip=["country_domain", "category", "count"],
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Country Domain and Category",
            subtitle="For the Top 6 to 30 Country Domain",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor="middle",
        ),
        height=500,
    )
)

st.altair_chart(bar, use_container_width=True)


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
st.dataframe(dmt_by_category_over_time)
st.dataframe(dmt_by_category_over_time.dtypes)

# Schema
# source	int64
# target	int64
# value	int64
# labels	object

dmt_by_category_over_time["month"] = (
    dmt_by_category_over_time["month"].astype(str).str[:7]
)

# Charts bar stacked
bar = (
    alt.Chart(dmt_by_category_over_time)
    .mark_bar()
    .encode(
        x=alt.X("month", title="Month"),
        y=alt.Y("count", title="Count"),
        color=alt.Color("category", legend=None),
        tooltip=["category", "count"],
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Category over Time",
            subtitle="Stacked",
            subtitleColor="grey",
        ),
        height=600,
    )
)

st.altair_chart(bar, use_container_width=True)


# # Sankey
# data = dict(
#     type="sankey",
#     node=dict(
#         pad=21,
#         thickness=15,
#         line=dict(color="black", width=0.6),
#         label=dmt_by_category_over_time["labels"],
#         color=dmt_by_category_over_time["labels"].apply(
#             lambda x: "lightblue" if "by" in x else "lightgreen"
#         ),
#     ),
#     link=dict(
#         source=dmt_by_category_over_time["source"],
#         target=dmt_by_category_over_time["target"],
#         value=dmt_by_category_over_time["value"],
#         color=dmt_by_category_over_time["source"].apply(
#             lambda x: "orange" if x == 0 else "blue"
#         ),
#     ),
# )

# fig = go.Figure(
#     data,
#     layout=dict(
#         title=dict(
#             text="Blocked Sites by Category over Time",
#             pad=dict(l=20),
#             y=0.99,
#             subtitle=dict(
#                 text="Sankey Diagram",
#             ),
#         ),
#         font_size=16,
#         height=680,
#     ),
# )
# st.plotly_chart(fig, use_container_width=True)
