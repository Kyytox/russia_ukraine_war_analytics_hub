import datetime as dt
import pandas as pd

import streamlit as st

import altair as alt
import plotly.graph_objects as go

from variables import path_dmt_block_site
from utils import jump_lines, init_css, add_analytics_tag, developper_link

# Google Analytics
add_analytics_tag()

# CSS
init_css()


dmt_global = pd.read_parquet(f"{path_dmt_block_site}/dmt_global.parquet")
dmt_by_date = pd.read_parquet(f"{path_dmt_block_site}/dmt_by_date.parquet")
dmt_by_metrics_over_time = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_metrics_over_time.parquet"
)
dmt_by_authority_category = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_authority_category.parquet"
)
dmt_by_country_category = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_country_by_category.parquet"
)

dmt_all_data = pd.read_parquet(f"{path_dmt_block_site}/dmt_all_data.parquet")


# List of top 30 country domain
LIST_TOP_COUNTRY = (
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]
    .sort_values(by="count", ascending=False)
    .metric.tolist()
)

# List of top banning authority
list_ban_authority = dmt_global[dmt_global["type_metric"] == "by_banning_authority"][
    "metric"
].unique()


# DF TREE MAP
# keep only the top 6 country
df_tree = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_COUNTRY[:6])
]


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

        dmt_by_country_category = dmt_by_country_category[
            dmt_by_country_category["country_domain"].isin(selected_countries)
        ]

        dmt_by_metrics_over_time = dmt_by_metrics_over_time[
            (dmt_by_metrics_over_time["type_metric"] != "by_country_domain")
            | (dmt_by_metrics_over_time["metric"].isin(selected_countries))
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

    developper_link()


###############
## MAIN PAGE ##
###############

st.title("Blocked Websites in Russia", anchor="Blocked Websites in Russia")
st.error(
    """
    The data has been collected from **[Websites Blocked in Russia Since February 2022](https://www.top10vpn.com/research/websites-blocked-in-russia/)**
    
    This page is a graphical representation of the data.
    """,
    icon="⚠️",
)


##############
## OVERVIEW ##
##############

jump_lines(1)
st.header("Overview of Blocked Websites in Russia", divider="grey")
jump_lines(3)


# Cards for global metrics
col1, col2, col3, col4 = st.columns(4)

# Max number of sites blocked
col1.metric(
    "Total Websites Blocked",
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]["count"].sum(),
)

# Year with the most sites blocked
max_year = (
    dmt_by_date[dmt_by_date["type_metric"] == "by_year"].nlargest(1, "count").iloc[0]
)
col2.metric(
    f"Year with the most websites blocked : **{max_year['date'].date().year}**",
    max_year["count"],
)

# Category with the most sites blocked
max_category = (
    dmt_global[dmt_global["type_metric"] == "by_category"].nlargest(1, "count").iloc[0]
)
col3.metric(
    f"Category most blocked : **{max_category['metric']}**",
    max_category["count"],
)

# Banning authority the most active
max_authority = (
    dmt_global[dmt_global["type_metric"] == "by_banning_authority"]
    .nlargest(1, "count")
    .iloc[0]
)
col4.metric(
    f"Most active banning authority : **{max_authority['metric']}**",
    max_authority["count"],
)


jump_lines(3)


########################################
##      BLOCKED SITES BY COUNTRY      ##
##    BLOCKED SITES BY SUBCATEGORY    ##
##     BLOCKED SITES BY CATEGORY      ##
## BLOCKED SITES BY BANNING AUTHORITY ##
##             CHARTS BAR             ##
########################################
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

        dict_infos = {
            "by_country_domain": {
                "color": "purples",
                "title": "Countries With Most Blocked Domains",
                "subtitle": "Top 30 Countries",
                "y_axe": "Country",
            },
            "by_subcategory": {
                "color": "viridis",
                "title": "Subcategories of Websites Blocked in Russia",
                "subtitle": "Top 30 Subcategories",
                "y_axe": "Subcategory",
            },
            "by_category": {
                "color": "plasma",
                "title": "Categories of Websites Blocked",
                "subtitle": "",
                "y_axe": "Category",
            },
            "by_banning_authority": {
                "color": "magma",
                "title": "Russian authorities most active in censoring websites",
                "subtitle": "",
                "y_axe": "Banning Authority",
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
                y=alt.Y("metric", sort="-x", title=dict_infos[type_metric]["y_axe"]),
                x="count",
                color=alt.Color(
                    "metric",
                    legend=None,
                    scale=alt.Scale(scheme=dict_infos[type_metric]["color"]),
                    sort=alt.EncodingSortField("count", order="ascending"),
                ),
                tooltip=["metric", "count"],
            )
            .properties(
                title=alt.TitleParams(
                    dict_infos[type_metric]["title"],
                    anchor=None,
                    subtitle=dict_infos[type_metric]["subtitle"],
                    subtitleColor="grey",
                    subtitleFontSize=14,
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
                        height=300,
                    )

                with subcol2:
                    # resume what is miroor subcategory
                    if type_metric == "by_subcategory":
                        st.write(
                            """'Mirror' subcategory refers to alternative domain names created to bypass Russian censorship.
                            These are primarily used by news services like Radio Liberty/Radio Free Europe (RL/RFE) to maintain 
                            access when their main domains are blocked. When Roskomnadzor blocks one mirror domain, new ones 
                            are quickly created and shared through social media."""
                        )

        jump_lines(3)

st.divider()
jump_lines(3)


#################################################
## COUNTRIES WITH AT LEAST ONE BLOCKED WEBSITE ##
##                 CHARTS MAP                  ##
#################################################

df = dmt_global[dmt_global["type_metric"] == "by_country_domain"].sort_values(
    by="count", ascending=False
)

fig = go.Figure(
    data=go.Choropleth(
        locations=df["metric"],
        locationmode="country names",
        z=df["count"],
        showscale=False,
        colorscale="sunset",
        hoverinfo="location+z",
    )
)

fig.update_layout(
    geo=dict(
        scope="world",
        projection=go.layout.geo.Projection(type="equirectangular", scale=1),
        showcountries=True,
        countrycolor="black",
        showocean=True,
        oceancolor="LightBlue",
    ),
    title=dict(
        text="Countries where websites are blocked in Russia",
        font=dict(size=16),
        x=0,
        y=1,
    ),
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    width=1200,
    height=680,
    dragmode=False,
)
st.plotly_chart(fig)
st.divider()
jump_lines(3)


#############################
## BLOCKED SITES OVER TIME ##
#############################
st.header("Blocked Sites in Russia Over Time", divider="grey")
jump_lines(3)

col1, col2 = st.columns([0.5, 0.5])

###########################
## BLOCKED SITES BY YEAR ##
##      CHARTS BAR       ##
###########################
df = dmt_by_date[dmt_by_date["type_metric"] == "by_year"]
df["date"] = df["date"].astype(str).str[:4]
year = (
    alt.Chart(df)
    .mark_bar(
        cornerRadius=4,
    )
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
            "Blocked Websites by Year",
            anchor=None,
            subtitle="In Russia since 2022",
            subtitleColor="grey",
            subtitleFontSize=14,
        ),
        height=500,
    )
)

col1.altair_chart(year, use_container_width=True)


############################
## BLOCKED SITES BY MONTH ##
##       CHARTS BAR       ##
############################
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
            "Blocked Websites by Month",
            subtitle="In Russia since 2022",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor=None,
        ),
        height=500,
    )
)

col2.altair_chart(month, use_container_width=True)
jump_lines(4)


########################
## BLOXK SITES BY DAY ##
##     CHARTS BAR     ##
########################
col1, col2 = st.columns([0.5, 0.5])

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
            "Blocked Websites by Day",
            subtitle="In Russia since 2022",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor=None,
        ),
    )
)

upper = base.encode(alt.X("date:T", title="Day", scale=alt.Scale(domain=brush)))
lower = base.properties(height=110, title="").add_params(brush)

col1.altair_chart(upper & lower, use_container_width=True)


###################################
## BLOCKED SITES, DAY CUMULATIVE ##
##          CHARTS LINE          ##
###################################
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
        title=alt.TitleParams(
            "Cumulative Total of Blocked Websites Over Time",
            subtitle="In Russia since 2022",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor=None,
        ),
        height=710,
    )
)

col2.altair_chart(day_cumul, use_container_width=True)
jump_lines(4)


########################
## BLOCK SITES BY DAY ##
##   CHARTS HEATMAP   ##
########################
col1, col2 = st.columns([0.5, 0.5], gap="medium")
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
                f"Blocked Websites by Day in {year}",
                anchor=None,
            ),
        )
    ).configure_axis(domain=False)

    if year == "2022" or year == "2024":
        col1.altair_chart(day_rect, use_container_width=True)
    else:
        col2.altair_chart(day_rect, use_container_width=True)


with col2:
    html_content = "<div style='margin-left: 60px;'>"
    html_content += "<p style='margin-bottom: 2px;'> Day with the most websites blocked for each year</p>"
    html_content += "</div>"
    st.html(html_content)

    html_content = "<div style='margin-left: 90px;'>"
    for year in dmt_by_date["date"].dt.year.unique():
        year_data = dmt_by_date[
            (dmt_by_date["date"].dt.year == year)
            & (dmt_by_date["type_metric"] == "by_day")
        ]
        max_date = year_data["date"].loc[year_data["count"].idxmax()]
        count_for_day = year_data["count"].max()
        html_content += f"<p>- <strong>{max_date.date().strftime('%B %d %Y')}</strong>, avec <strong>{count_for_day}</strong> sites bloqués</p>"
    html_content += "</div>"

    st.html(html_content)

st.divider()


################################################
##  BLOCK SITES BY MONTH, BY COUNTRY DOMAIN   ##
## BLOCK SITES BY MONTH, BY BANNING AUTHORITY ##
################################################
col1, col2 = st.columns([0.46, 0.54], gap="medium")

for type_metric in dmt_by_metrics_over_time["type_metric"].unique():

    df = dmt_by_metrics_over_time[
        dmt_by_metrics_over_time["type_metric"] == type_metric
    ]
    df["month"] = df["month"].astype(str).str[:7]

    dict_infos = {
        "by_country_domain": {
            "color": "tableau20",
            "title": "Website Blocking Trends by Country",
            "subtitle": "Monthly distribution of blocked domains by country",
        },
        "by_banning_authority": {
            "color": "magma",
            "title": "Censorship Activity by Russian Authorities",
            "subtitle": "Monthly breakdown of website blocks by regulatory body",
        },
        "by_category": {
            "color": "category20b",
            "title": "Content Categories Targeted for Blocking",
            "subtitle": "Monthly distribution of blocked websites by content type",
        },
    }

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
                scale=alt.Scale(scheme=dict_infos[type_metric]["color"]),
                legend=alt.Legend(
                    orient=(
                        "right"
                        if (
                            type_metric == "by_country_domain"
                            or type_metric == "by_category"
                        )
                        else "top"
                    ),
                    title=None,
                    values=df["metric"].value_counts().nlargest(20).index.tolist(),
                ),
            ),
            tooltip=["metric", "count"],
            order=alt.Order("count", sort="descending"),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.4)),
        )
        .properties(
            title=alt.TitleParams(
                dict_infos[type_metric]["title"],
                subtitle=dict_infos[type_metric]["subtitle"],
                subtitleColor="grey",
                subtitleFontSize=14,
                anchor=None,
            ),
            height=700,
        )
        .add_params(selection)
    )

    if type_metric == "by_category":
        col2.altair_chart(bar, use_container_width=True)
    elif type_metric == "by_banning_authority":
        col1.altair_chart(bar, use_container_width=True)
    else:
        st.altair_chart(bar, use_container_width=True)

    jump_lines(2)


#########################
## COMPARAISON METRICS ##
#########################

st.header("Blocked Websites by Different Metrics Comparison", divider="grey")
col1, col2 = st.columns([0.5, 0.5])

###############################################################
## RELATION BETWEEN AUTHORITY AND CATEGORY FOR BLOCKED SITES ##
##                       CHARTS SANKEY                       ##
###############################################################
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
            text="Mapping Censorship: Authorities and Targeted Content Categories",
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text="Relationship between Russian authorities and the types of websites they block",
                font_size=14,
                font_color="grey",
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

# Colors for each country
colors_country = {
    "United States": "#9e6e54",
    "Russia": "#f3cf60",
    "Unknown": "#dbbaab",
    "Ukraine": "#aa6795",
    "United Kingdom": "#e0afd2",
    "Belarus": "#4fa24d",
}

# add labes cols
df_tree["labels"] = df_tree["category"] + " - " + df_tree["count"].astype(str)


# Charts Tree Map (altair rect
base = (
    alt.Chart(df_tree)
    .transform_aggregate(
        count_="sum(count)",
        groupby=["country_domain", "category", "labels"],
    )
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


# Display category
text_category = base.mark_text(baseline="middle", fontSize=11.5).encode(
    x=alt.X("xc:Q").axis(None),
    y=alt.Y("yc:Q").title(None),
    text=f"labels:N",
    color=alt.value("white"),
    tooltip=[
        alt.Tooltip("country_domain:N", title="Country"),
        alt.Tooltip("category:N", title="Category"),
        alt.Tooltip("count_:Q", title="Count", format=","),
    ],
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
            "Top Blocked Website Categories by Country in Russia",
            subtitle="For the Top 6 Country",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor=None,
        ),
    )
    .configure_view(stroke=None)
    .configure_axis(domain=False, ticks=False, labels=False, grid=False)
)

st.altair_chart(final_chart, use_container_width=True)

jump_lines()

###################################################
## TOP 6 TO 30 COUNTRY SITES BLOCKED BY CATEGORY ##
##              CHARTS BAR STACKED               ##
###################################################

df = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_COUNTRY[6:])
]

selection = alt.selection_point(fields=["category"], bind="legend")

bar = (
    alt.Chart(df)
    .mark_bar(
        cornerRadius=2,
    )
    .encode(
        x=alt.X("country_domain", title="Country", sort="-y"),
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
            "Blocked Websites by Country and Category",
            subtitle="For the rest of the countries",
            subtitleColor="grey",
            subtitleFontSize=14,
            anchor=None,
        ),
        height=700,
    )
    .add_params(selection)
)

st.altair_chart(bar, use_container_width=True)

st.divider()
jump_lines(3)


########################################
## EXPLORE WEBSITE BLOCKED BY COUNTRY ##
########################################

col1, col2 = st.columns([0.4, 0.6])

with col1:
    st.subheader("Explore Websites Blocked by Country")
    slt_country = st.selectbox(
        "Select a country",
        dmt_all_data["country_domain"].unique(),
        index=0,
    )

    df = dmt_all_data[dmt_all_data["country_domain"] == slt_country]

    st.dataframe(
        df[["domain", "category", "banning_authority", "date_blocked"]].rename(
            columns={
                "domain": "Domain",
                "category": "Category",
                "banning_authority": "Banning Authority",
                "date_blocked": "Date Blocked",
            }
        ),
        hide_index=True,
        use_container_width=True,
        height=600,
    )

with col2:
    st.subheader("Find if a website is blocked in Russia")
    domain = st.text_input("Enter a domain", "youtube")

    if st.button("Check"):

        df = dmt_all_data[dmt_all_data["domain"].str.contains(domain)]

        if len(df) > 0:
            st.success(
                f"**{domain}** was blocked in **Russia** on **{dmt_all_data['date_blocked'].iloc[0].date().strftime('%d %B %Y')}**."
            )
            st.dataframe(
                df[["domain", "category", "banning_authority", "date_blocked"]].rename(
                    columns={
                        "domain": "Domain",
                        "category": "Category",
                        "banning_authority": "Banning Authority",
                        "date_blocked": "Date Blocked",
                    }
                ),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.warning(f"**{domain}** is not blocked in **Russia**.")


st.divider()
jump_lines(3)
