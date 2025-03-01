from bs4 import BeautifulSoup
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import shutil
import pathlib
import altair as alt
import plotly.graph_objects as go

import streamlit_shadcn_ui as ui

from utils import jump_lines, init_css, add_analytics_tag, developper_link

# Google Analytics
add_analytics_tag()

# CSS
init_css()


path = "data/data_warehouse/datamarts/russia_block_sites"


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


LIST_TOP_30 = (
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]
    .sort_values(by="count", ascending=False)
    .head(30)
    .metric.tolist()
)

list_ban_authority = dmt_global[dmt_global["type_metric"] == "by_banning_authority"][
    "metric"
].unique()


#############
## SIDEBAR ##
#############
with st.sidebar:

    # Create sidebar filters
    st.header("Filters")

    # Country domain filter
    country_domains = dmt_by_country_category["country_domain"].unique()
    country_domains = country_domains.tolist()
    selected_countries = st.multiselect(
        "Select Countries",
        options=country_domains,
        placeholder="All",
    )

    # Category filter from dmt_global
    categories = dmt_global[dmt_global["type_metric"] == "by_category"][
        "metric"
    ].unique()
    categories = categories.tolist()
    selected_categories = st.multiselect(
        "Select Categories",
        options=categories,
        placeholder="All",
    )

    # Subcategory filter from dmt_global
    subcategories = dmt_global[dmt_global["type_metric"] == "by_subcategory"][
        "metric"
    ].unique()
    subcategories = subcategories.tolist()
    selected_subcategories = st.multiselect(
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

        # dmt_by_country_category = dmt_by_country_category[
        #     dmt_by_country_category["country_domain"].isin(selected_countries)
        # ]

        dmt_by_date_and_metrics = dmt_by_date_and_metrics[
            (dmt_by_date_and_metrics["type_metric"] != "by_country_domain")
            | (dmt_by_date_and_metrics["metric"].isin(selected_countries))
        ]

    if len(selected_categories) > 0:
        dmt_global = dmt_global[
            (dmt_global["type_metric"] != "by_category")
            | (dmt_global["metric"].isin(selected_categories))
        ]
        # dmt_by_country_category = dmt_by_country_category[
        #     dmt_by_country_category["category"].isin(selected_categories)
        # ]
        dmt_by_category_over_time = dmt_by_category_over_time[
            dmt_by_category_over_time["category"].isin(selected_categories)
        ]

    if len(selected_subcategories) > 0:
        dmt_global = dmt_global[
            (dmt_global["type_metric"] != "by_subcategory")
            | (dmt_global["metric"].isin(selected_subcategories))
        ]

    developper_link()


###############
## MAIN PAGE ##
###############

st.title("Blocked Sites in Russia")
st.error(
    """
    The data has been collected from **[Websites Blocked in Russia Since February 2022](https://www.top10vpn.com/research/websites-blocked-in-russia/countries-with-most-blocked-domains/)**
    
    This page is a graphical representation of the data.
    """,
    icon="⚠️",
)


##############
## OVERVIEW ##
##############

jump_lines(1)
st.header("Overview of Blocked Sites in Russia", divider="grey")
jump_lines(3)


# Cards for global metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "Total Sites Blocked",
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]["count"].sum(),
    "sites",
)
col2.metric(
    "Total Categories",
    dmt_global[dmt_global["type_metric"] == "by_category"]["count"].sum(),
    "categories",
)
col3.metric(
    "Total Subcategories",
    dmt_global[dmt_global["type_metric"] == "by_subcategory"]["count"].sum(),
    "subcategories",
)
col4.metric(
    "Total Authorities",
    dmt_global[dmt_global["type_metric"] == "by_banning_authority"]["count"].sum(),
    "authorities",
)
jump_lines(3)


lst_type_metric = [
    dmt_global["type_metric"].unique().tolist()[i : i + 2] for i in range(0, 4, 2)
]

for group_tm in lst_type_metric:
    col1, col2 = st.columns([0.5, 0.5])
    curr_col = col2

    for type_metric in group_tm:

        df = dmt_global[dmt_global["type_metric"] == type_metric].sort_values(
            by="count", ascending=False
        )

        dict_colors = {
            "by_country_domain": {
                "color": "purples",
                "subtitle": "Top 30",
            },
            "by_subcategory": {
                "color": "viridis",
                "subtitle": "Top 30",
            },
            "by_category": {
                "color": "plasma",
                "subtitle": "",
            },
            "by_banning_authority": {
                "color": "magma",
                "subtitle": "",
            },
        }

        # Charts bar
        bar = (
            alt.Chart(df.head(30))
            .mark_bar(
                cornerRadius=4,
                orient="horizontal",
            )
            .encode(
                y=alt.Y("metric", sort="-x"),
                x="count",
                color=alt.Color(
                    "metric",
                    legend=None,
                    scale=alt.Scale(scheme=dict_colors[type_metric]["color"]),
                    sort=alt.EncodingSortField("count", order="ascending"),
                ),
                tooltip=["metric", "count"],
            )
            .properties(
                title=alt.TitleParams(
                    f"Blocked Sites {type_metric.title().replace('_', ' ')}",
                    # anchor="middle",
                    anchor=None,
                    subtitle=dict_colors[type_metric]["subtitle"],
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

            jump_lines(5)


st.divider()

###############
## OVER TIME ##
###############

st.header("Blocked Sites in Russia Over Time", divider="grey")
jump_lines(3)

col1, col2 = st.columns([0.5, 0.5])

# By Year
df = dmt_by_date[dmt_by_date["type_metric"] == "by_year"]
df["date"] = df["date"].astype(str).str[:4]
year = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        alt.X("date", title="Year", axis=alt.Axis(labelAngle=0)),
        alt.Y("count", title="Count"),
        color=alt.Color(
            "date:N",
            scale=alt.Scale(
                domain=["2022", "2023", "2024"], range=["#079999", "#054985", "#8f0a10"]
            ),
            legend=alt.Legend(orient="top-right", title=None, direction="horizontal"),
        ),
        tooltip=["date", "count"],
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Year",
            anchor=None,
        ),
        height=500,
    )
)

col1.altair_chart(year, use_container_width=True)


# By Month
df = dmt_by_date[dmt_by_date["type_metric"] == "by_month"]
df["date"] = df["date"].astype(str).str[:7]
df["year"] = df["date"].str[:4]
month = (
    alt.Chart(df)
    .mark_bar(
        cornerRadius=4,
    )
    .encode(
        alt.X("date", title="Month"),
        alt.Y("count", title="Count"),
        color=alt.Color(
            "year:N",
            scale=alt.Scale(
                domain=["2022", "2023", "2024"], range=["#079999", "#054985", "#8f0a10"]
            ),
            legend=alt.Legend(orient="top-right", title=None, direction="horizontal"),
        ),
        tooltip=["date", "count"],
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Month",
            anchor=None,
        ),
        height=500,
    )
)

col2.altair_chart(month, use_container_width=True)
jump_lines(4)


col1, col2 = st.columns([0.5, 0.5])

# Day (line)
import datetime as dt

date_range = (dt.date(2022, 1, 1), dt.date(2024, 12, 31))
brush = alt.selection_interval(encodings=["x"], value={"x": date_range})

df = dmt_by_date[dmt_by_date["type_metric"] == "by_day"]
base = (
    alt.Chart(df, width=600, height=400)
    .mark_bar(
        size=2,
    )
    .encode(
        x="date:T",
        y="count:Q",
        color=alt.Color("type_metric", legend=None, scale=alt.Scale(range=["#b8471a"])),
        tooltip=["date", "count"],
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Day",
            anchor=None,
        ),
    )
)

upper = base.encode(alt.X("date:T", title="Day", scale=alt.Scale(domain=brush)))
lower = base.properties(height=110, title="").add_params(brush)

col1.altair_chart(upper & lower, use_container_width=True)


# Bt Day cuml
df_day_cumul = dmt_by_date[dmt_by_date["type_metric"] == "by_day_cumul"]
day_cumul = (
    alt.Chart(df_day_cumul)
    .mark_line()
    .encode(
        x="date",
        y="count",
        color=alt.Color("type_metric", legend=None, scale=alt.Scale(range=["#b8471a"])),
        tooltip=["date", "count"],
    )
    .properties(
        title=alt.TitleParams("Blocked Sites by Day Cumulative", anchor=None),
        height=710,
    )
)

col2.altair_chart(day_cumul, use_container_width=True)
jump_lines(4)


col1, col2 = st.columns([0.5, 0.5], gap="medium")
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
            title=alt.TitleParams(
                f"Blocked Sites by Day in {year}",
                subtitle="For each day of the year",
                subtitleColor="grey",
                anchor=None,
            ),
        )
    ).configure_axis(domain=False)

    if year == "2022" or year == "2024":
        col1.altair_chart(day_rect, use_container_width=True)
    else:
        col2.altair_chart(day_rect, use_container_width=True)

with col2:
    st.write("Day with the most sites blocked for each year")
    for year in dmt_by_date["date"].dt.year.unique():
        year_data = dmt_by_date[
            (dmt_by_date["date"].dt.year == year)
            & (dmt_by_date["type_metric"] == "by_day")
        ]
        max_date = year_data["date"].loc[year_data["count"].idxmax()]
        count_for_day = year_data["count"].max()
        st.write(
            f"- **{max_date.date().strftime('%B %d %Y')}**, with **{count_for_day}** sites blocked"
        )


st.divider()

#########################
## COMPARAISON METRICS ##
#########################

st.header("Blocked Sites by Different Metrics Comparison", divider="grey")
col1, col2 = st.columns([0.5, 0.5])


# Block sites by month, by country domain
# Block sites by month, by banning authority
for type_metric in dmt_by_date_and_metrics["type_metric"].unique():
    df = dmt_by_date_and_metrics[dmt_by_date_and_metrics["type_metric"] == type_metric]
    df["month"] = df["month"].astype(str).str[:7]

    if type_metric == "by_country_domain":
        color = "tableau20"
    else:
        color = "magma"

    selection = alt.selection_point(fields=["metric"], bind="legend")

    # Bar
    bar = (
        alt.Chart(df)
        .mark_bar(
            cornerRadius=4,
        )
        .encode(
            x=alt.X("month", title="Month"),
            y=alt.Y("count", title="Count"),
            color=alt.Color(
                "metric",
                scale=alt.Scale(scheme=color),
                legend=alt.Legend(
                    orient="right" if type_metric == "by_country_domain" else "top",
                    title=None,
                    values=df["metric"].value_counts().nlargest(14).index.tolist(),
                ),
            ),
            tooltip=["metric", "count"],
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.4)),
        )
        .properties(
            title=f"Blocked Sites by Month {type_metric.title().replace('_', ' ')}",
            height=700,
        )
        .add_params(selection)
    )

    if type_metric == "by_country_domain":
        col1.altair_chart(bar, use_container_width=True)
    else:
        col2.altair_chart(bar, use_container_width=True)

    jump_lines(2)

st.divider()

# Sankey
color_src = {0: "#3c116f", 1: "#7d2470", 2: "#be3954", 3: "#ff9f6f"}
color_label = {
    "Office of the Prosecutor General": "#be3954",
    "Minstry of Foreign Affairs": "#3c116f",
    "Not specified": "#7d2470",
    "Roskomnadzor": "#ff9f6f",
}


# data for sankey
data = dict(
    type="sankey",
    node=dict(
        pad=21,
        thickness=15,
        line=dict(color="black", width=0.6),
        label=dmt_by_authority_category["labels"],
        color=dmt_by_authority_category["labels"].apply(
            lambda x: color_label[x] if x in color_label else "gray"
        ),
    ),
    link=dict(
        source=dmt_by_authority_category["source"],
        target=dmt_by_authority_category["target"],
        value=dmt_by_authority_category["value"],
        color=dmt_by_authority_category["source"].apply(
            lambda x: color_src[x] if x in color_src else "grey"
        ),
    ),
)

# Sankey Diagram
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


st.divider()


#############################################
## TOP 6 COUNTRY SITES BLOCKED BY CATEGORY ##
##             CHARTS TREE MAP             ##
#############################################

# keep only the top 6 country
df = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_30[:6])
]

# Colors for each country
colors_country = {
    "United States": "#9e6e54",
    "Russia": "#f3cf60",
    "Unknown": "#dbbaab",
    "Ukraine": "#aa6795",
    "United Kingdom": "#e0afd2",
    "Belarus": "#4fa24d",
}

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

# Rect for each country
rect = (
    base.mark_rect()
    .encode(
        x=alt.X("nx:Q").axis(None),
        x2="nx2",
        y=alt.Y("ny:Q").axis(None),
        y2="ny2",
        color=alt.Color(
            "country_domain:N",
            scale=alt.Scale(
                domain=list(colors_country.keys()), range=list(colors_country.values())
            ),
            legend=None,
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

# Display count
# text_count = base.mark_text(baseline="middle", fontSize=11).encode(
#     x=alt.X("xc:Q").axis(None),
#     y=alt.Y("yc:Q").title(None),
#     text="count_:Q",
#     color=alt.value("white"),
# )

# Display category
text_category = base.mark_text(baseline="middle", fontSize=11).encode(
    x=alt.X("xc:Q").axis(None),
    y=alt.Y("yc:Q").title(None),
    text="category:N",
    color=alt.value("white"),
)

# Display country
country_labels = base.mark_text(baseline="middle", align="center", fontSize=12).encode(
    x=alt.X("min(xc):Q").title("Country Domain").axis(orient="top"),
    color=alt.Color("country_domain:N", legend=None),
    text="country_domain:N",
)

# Final chart
final_chart = (
    (country_labels & (rect + text_category))
    .resolve_scale(x="shared")
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Country Domain and Category",
            subtitle="For the Top 6 Country",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor="middle",
        ),
    )
    .configure_view(stroke=None)
    .configure_axis(domain=False, ticks=False, labels=False, grid=False)
)

st.altair_chart(final_chart, use_container_width=True)


###################################################
## TOP 6 TO 30 COUNTRY SITES BLOCKED BY CATEGORY ##
##              CHARTS BAR STACKED               ##
###################################################

df = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_30[6:])
]

selection = alt.selection_point(fields=["category"], bind="legend")

bar = (
    alt.Chart(df)
    .mark_bar(
        cornerRadius=2,
    )
    .encode(
        x=alt.X("country_domain", title="Country Domain", sort="-y"),
        y=alt.Y("count", title="Count"),
        color=alt.Color(
            "category",
            scale=alt.Scale(scheme="category20c"),
        ),
        tooltip=["country_domain", "category", "count"],
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.35)),
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Country Domain and Category",
            subtitle="For the Top 6 to 30 Country",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor="middle",
        ),
        height=600,
    )
    .add_params(selection)
)

st.altair_chart(bar, use_container_width=True)
jump_lines(2)

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

#########################################
## BLOCKED SITES BY CATEGORY OVER TIME ##
##         CHARTS BAR STACKED          ##
#########################################

dmt_by_category_over_time["month"] = (
    dmt_by_category_over_time["month"].astype(str).str[:7]
)

selection = alt.selection_point(fields=["category"], bind="legend")

# Charts bar stacked
bar = (
    alt.Chart(dmt_by_category_over_time)
    .mark_bar(
        cornerRadius=2,
    )
    .encode(
        x=alt.X("month", title="Month"),
        y=alt.Y("count", title="Count"),
        color=alt.Color(
            "category",
            scale=alt.Scale(scheme="category20b"),
        ),
        tooltip=["category", "count"],
        order=alt.Order("count", sort="descending"),
        opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.4)),
    )
    .properties(
        title=alt.TitleParams(
            "Blocked Sites by Category over Time",
            subtitle="",
            subtitleColor="grey",
        ),
        height=700,
    )
    .add_params(selection)
)

st.altair_chart(bar, use_container_width=True)
