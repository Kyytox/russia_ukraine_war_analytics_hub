import pandas as pd

import dash
from dash import Dash, Input, Output, callback, html, dcc
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.express as px

from assets.components.warning_sources import warning_sources
from assets.components.figure_chart import box_chart


from utils.variables import PATH_DMT_BLOCK_SITE
from utils.variables_charts import (
    MONTHS,
    COLORS_BLOCK_SITE,
)
from utils.utils_charts import fig_upd_layout
from utils.generate_chart import (
    generate_treemap,
    generate_sunburst,
    generate_bar,
    generate_line,
    generate_heatmap,
    generate_stacked_bar,
    generate_map,
)

dash.register_page(__name__)


dmt_global = pd.read_parquet(f"{PATH_DMT_BLOCK_SITE}/dmt_global.parquet")
dmt_by_date = pd.read_parquet(f"{PATH_DMT_BLOCK_SITE}/dmt_by_date.parquet")

# New files with pre-calculated pivots
dmt_by_authority_over_time = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_authority_over_time.parquet"
)
dmt_by_category_over_time = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_category_over_time.parquet"
)
dmt_by_country_over_time = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_country_over_time.parquet"
)
dmt_by_authority_category = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_authority_category.parquet"
)
dmt_by_country_category_top_6 = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_country_by_category_top_6.parquet"
)
dmt_by_country_category_after_6 = pd.read_parquet(
    f"{PATH_DMT_BLOCK_SITE}/dmt_by_country_by_category_after_6.parquet"
)

dmt_all_data = pd.read_parquet(f"{PATH_DMT_BLOCK_SITE}/dmt_all_data.parquet")


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


markdown_text = """
The data has been collected from **[Websites Blocked in Russia Since February 2022](https://www.top10vpn.com/research/websites-blocked-in-russia/)**

This page is a graphical representation of the data.
"""


#########################################
#########################################
# Create global metrics cards
total_blocked = dmt_global[dmt_global["type_metric"] == "by_country_domain"][
    "count"
].sum()

# Year
max_year_data = (
    dmt_by_date[dmt_by_date["type_metric"] == "by_year"].nlargest(1, "count").iloc[0]
)

# Category
max_category_data = (
    dmt_global[dmt_global["type_metric"] == "by_category"].nlargest(1, "count").iloc[0]
)

# Authority
max_authority_data = (
    dmt_global[dmt_global["type_metric"] == "by_banning_authority"]
    .nlargest(1, "count")
    .iloc[0]
)

# Min - Max dates
min_date = dmt_by_date["date"].min()
max_date = dmt_by_date["date"].max()


# Layout of the page
layout = html.Div(
    className="page-content",
    children=[
        html.H1(
            className="page-title",
            children="🚫 Websites Blocked in Russia",
        ),
        warning_sources(markdown_text),
        html.Div(
            className="div-group-chart",
            children=[
                html.Div(
                    className="div-group-metrics",
                    children=[
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3(f"{total_blocked}"),
                                html.P("Blocked Sites"),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3(f"{max_year_data["date"].date().year}"),
                                html.P(
                                    f"Year with the most blocks ({max_year_data["count"]})"
                                ),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3(f"{max_category_data["metric"]}"),
                                html.P(
                                    f"Most blocked category ({max_category_data["count"]})"
                                ),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3(f"{max_authority_data["metric"]}"),
                                html.P(
                                    f"Most active authority ({max_authority_data["count"]})"
                                ),
                            ],
                        ),
                    ],
                    style={"width": "14%"},
                ),
                box_chart("pg_block_fig_bar_country", {}, "41%", "75vh"),
                box_chart("pg_block_fig_bar_sub_cat", {}, "41%", "75vh"),
            ],
        ),
        # Second row of charts - Category and authority
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_bar_cat", {}, "49%", "50vh"),
                box_chart("pg_block_fig_bar_auth", {}, "49%", "50vh"),
            ],
        ),
        # World map
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_map", {}, "100%", "80vh"),
            ],
        ),
        # Time trends section
        html.Div(
            className="section-title",
            children=[
                html.H2(
                    "Evolution of Website Blocking Over Time",
                    style={"marginTop": "40px"},
                ),
            ],
        ),
        # Year and month charts
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_year", {}, "26.5%", "60vh"),
                box_chart("pg_block_fig_month", {}, "72.5%", "60vh"),
            ],
        ),
        # Cumulative chart
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_day", {}, "61%", "60vh"),
                box_chart("pg_block_fig_cumul", {}, "38%", "60vh"),
            ],
        ),
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_heat_22", {}, "32.5%", "48vh"),
                box_chart("pg_block_fig_heat_23", {}, "32.5%", "48vh"),
                box_chart("pg_block_fig_heat_24", {}, "32.5%", "48vh"),
            ],
        ),
        # Add the new charts after the heatmaps
        html.Div(
            className="section-title",
            children=[
                html.H2(
                    "Blocking Trends by Country, Authority and Category",
                    style={"marginTop": "40px"},
                ),
            ],
        ),
        # Authority and category by month
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_auth_month", {}, "49%", "80vh"),
                box_chart("pg_block_fig_cat_month", {}, "49%", "80vh"),
            ],
        ),
        # Country by month (full width)
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_country_month", {}, "100%", "80vh"),
            ],
        ),
        # Relationship section
        html.Div(
            className="section-title",
            children=[
                html.H2(
                    "Relationships Between Authorities, Categories and Countries",
                    style={"marginTop": "40px"},
                ),
            ],
        ),
        # Sankey diagram
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_sankey", {}, "100%", "80vh"),
            ],
        ),
        # Treemap
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_treemap", {}, "100%", "80vh"),
            ],
        ),
        # Stacked bar chart for other countries
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_block_fig_bar_country_cat", {}, "100%", "80vh"),
            ],
        ),
    ],
)


# Callbacks
@callback(
    [
        Output("pg_block_fig_bar_country", "figure"),
        Output("pg_block_fig_bar_sub_cat", "figure"),
        Input("pg_block_fig_bar_country", "id"),
        Input("pg_block_fig_bar_sub_cat", "id"),
    ],
)
def group_1(f1, f2):

    #########################################
    #########################################
    # Countries With Most Blocked Domains
    # Bar Chart
    df = (
        dmt_global[dmt_global["type_metric"] == "by_country_domain"]
        .sort_values(by="count", ascending=True)
        .tail(30)
    )
    fig_bar_country = generate_bar(
        title="Countries With Most Blocked Domains",
        subtitle="",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#635d8d",
        orientation="h",
        hovertemplate="%{y}<br>%{x} blocked websites<extra></extra>",
    )
    fig_bar_country = fig_upd_layout(
        fig_bar_country,
        ygrid=False,
    )

    #########################################
    #########################################
    # Subcategories of Websites Blocked
    # Bar Chart
    df = (
        dmt_global[dmt_global["type_metric"] == "by_subcategory"]
        .sort_values(by="count", ascending=True)
        .tail(30)
    )
    fig_bar_sub_cat = generate_bar(
        title="Subcategories of Websites Blocked in Russia",
        subtitle="",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#5d8d7a",
        orientation="h",
        hovertemplate="%{y}<br>%{x} blocked websites<extra></extra>",
    )
    fig_bar_sub_cat = fig_upd_layout(
        fig_bar_sub_cat,
        ygrid=False,
    )

    return (fig_bar_country, fig_bar_sub_cat)


@callback(
    [
        Output("pg_block_fig_bar_cat", "figure"),
        Output("pg_block_fig_bar_auth", "figure"),
        Input("pg_block_fig_bar_cat", "id"),
        Input("pg_block_fig_bar_auth", "id"),
    ],
)
def group_2(f1, f2):

    #########################################
    #########################################
    # Categories of Websites Blocked
    # Bar Chart
    df = dmt_global[dmt_global["type_metric"] == "by_category"].sort_values(
        by="count", ascending=True
    )
    fig_bar_cat = generate_bar(
        title="Categories of Websites Blocked",
        subtitle="",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#8d725d",
        orientation="h",
        hovertemplate="%{y}<br>%{x} blocked websites<extra></extra>",
    )
    fig_bar_cat = fig_upd_layout(
        fig_bar_cat,
        ygrid=False,
    )

    #########################################
    #########################################
    # Banning authorities most active
    # Bar Chart
    df = dmt_global[dmt_global["type_metric"] == "by_banning_authority"].sort_values(
        by="count", ascending=True
    )
    fig_bar_auth = generate_bar(
        title="Russian authorities most active in censoring websites",
        subtitle="",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#8d5d5d",
        orientation="h",
        hovertemplate="%{y}<br>%{x} blocked websites<extra></extra>",
    )
    fig_bar_auth = fig_upd_layout(
        fig_bar_auth,
        ygrid=False,
    )

    return (fig_bar_cat, fig_bar_auth)


@callback(
    [
        Output("pg_block_fig_map", "figure"),
        Input("pg_block_fig_map", "id"),
    ],
)
def group_3(map):

    #########################################
    #########################################
    # World map showing countries with blocked websites
    # Map
    df_map = dmt_global[dmt_global["type_metric"] == "by_country_domain"]
    fig_map = generate_map(
        title="Countries where websites are blocked in Russia",
        subtitle="",
        polygons=None,
        loc=df_map["metric"],
        text_=df_map["metric"],
        z_=df_map["count"],
        locationmode="country names",
        colorbar_title="Nombre de sites",
        colorbar_titleside="top",
    )
    fig_map = fig_upd_layout(
        fig_map,
        dragmode=False,
    )

    return [fig_map]


@callback(
    [
        Output("pg_block_fig_year", "figure"),
        Output("pg_block_fig_month", "figure"),
        Input("pg_block_fig_year", "id"),
        Input("pg_block_fig_month", "id"),
    ],
)
def group_4(f1, f2):

    #########################################
    #########################################
    # Time trends
    # Bar chart
    df_year = dmt_by_date[dmt_by_date["type_metric"] == "by_year"]
    # df_year["date"] = df_year["date"].astype(str).str[:4]

    fig_year = generate_bar(
        title="Blocked Websites by Year",
        subtitle="In Russia since 2022",
        x_=df_year["date"],
        y_=df_year["count"],
        text=df_year["count"],
        colors=COLORS_BLOCK_SITE,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
    )
    fig_year = fig_upd_layout(
        fig_year,
        xgrid=False,
    )

    #########################################
    #########################################
    # Bar chart by month
    df_month = dmt_by_date[
        (dmt_by_date["type_metric"] == "by_month") & (dmt_by_date["count"] > 0)
    ]
    df_month["date"] = df_month["date"].astype(str).str[:7]
    df_month["year"] = df_month["date"].str[:4]

    fig_month = generate_bar(
        title="Blocked Websites by Month",
        subtitle="In Russia since 2022",
        x_=df_month["date"],
        y_=df_month["count"],
        text=df_month["count"],
        colors=COLORS_BLOCK_SITE,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
    )

    fig_month = fig_upd_layout(
        fig_month,
        title="Blocked Websites by Month",
        subtitle="In Russia since 2022",
        xgrid=False,
    )

    return [fig_year, fig_month]


@callback(
    [
        Output("pg_block_fig_day", "figure"),
        Output("pg_block_fig_cumul", "figure"),
        Input("pg_block_fig_day", "id"),
        Input("pg_block_fig_cumul", "id"),
    ],
)
def group_5(f1, f2):

    #########################################
    #########################################
    # Blocked websites by Day
    # Bar chart
    df_day = dmt_by_date[dmt_by_date["type_metric"] == "by_day"]
    df_day["date"] = df_day["date"].astype(str).str[:10]

    fig_day = generate_bar(
        title="Blocked Websites by Day",
        subtitle="In Russia since 2022",
        x_=df_day["date"],
        y_=df_day["count"],
        text=df_day["count"],
        colors="#b8471a",
        hovertemplate="%{x|%d %b %Y}<br>%{y} blocked websites<extra></extra>",
    )
    fig_day = fig_upd_layout(
        fig_day,
        xgrid=False,
    )

    #########################################
    #########################################
    # Line chart for cumulative blocks
    df_cumul = dmt_by_date[dmt_by_date["type_metric"] == "by_day_cumul"]
    fig_cumul = go.Figure(
        go.Scatter(
            x=df_cumul["date"],
            y=df_cumul["count"],
            mode="lines",
            line=dict(color="#b8471a", width=3),
            hovertemplate="%{x|%d %b %Y}<br>%{y} sites bloqués au total<extra></extra>",
            fill="tozeroy",
            fillcolor="rgba(184, 71, 26, 0.2)",
        )
    )
    fig_cumul = fig_upd_layout(
        fig_cumul,
        title="Cumulative Total of Blocked Websites Over Time",
        subtitle="In Russia since 2022",
    )

    return (fig_day, fig_cumul)


@callback(
    [
        Output("pg_block_fig_heat_22", "figure"),
        Output("pg_block_fig_heat_23", "figure"),
        Output("pg_block_fig_heat_24", "figure"),
        Input("pg_block_fig_heat_22", "id"),
        Input("pg_block_fig_heat_23", "id"),
        Input("pg_block_fig_heat_24", "id"),
    ],
)
def group_6(f1, f2, f3):
    #########################################
    #########################################
    # Blocked websites by day by year
    # Heatmap

    figures = []

    for year in [2022, 2023, 2024]:
        # Filter data for the specified year
        df = dmt_by_date[
            (dmt_by_date["type_metric"] == "by_day")
            & (dmt_by_date["date"].dt.year == year)
        ].sort_values(by="date", ascending=True)

        # Generate the Heatmap
        fig_heat = generate_heatmap(
            title=f"Blocked Websites by Day in {year}",
            subtitle=f"In Russia since {year}",
            x_=df["date"].dt.day,
            y_=df["date"].dt.month,
            z_=df["count"],
            hovertemplate="%{x} %{y}<br>%{z} blocked websites<extra></extra>",
        )

        # Update layout
        fig_heat = fig_upd_layout(
            fig_heat,
            xgrid=False,
            ygrid=False,
        )

        # Update axes
        fig_heat.update_yaxes(
            tickvals=list(range(1, 13)),
            ticktext=MONTHS,
        )
        fig_heat.update_xaxes(
            tickvals=list(range(1, 32)),
            ticktext=list(range(1, 32)),
            title_text="Day",
            title_font=dict(size=12),
        )

        figures.append(fig_heat)

    return figures


@callback(
    [
        Output("pg_block_fig_auth_month", "figure"),
        Output("pg_block_fig_cat_month", "figure"),
        Input("pg_block_fig_auth_month", "id"),
        Input("pg_block_fig_cat_month", "id"),
    ],
)
def group_7(f1, f2):
    # For the blocking authority, use the pivoted dataframe directly
    # Format the date for display
    df_auth = dmt_by_authority_over_time.copy()
    df_auth["month_str"] = pd.to_datetime(df_auth["month"]).dt.strftime("%Y-%m")

    print(dmt_by_authority_over_time)

    fig_auth_month = generate_stacked_bar(
        title="Censorship Activity by Russian Authorities",
        subtitle="Monthly breakdown of website blocks by regulatory body",
        df=df_auth,
        x_="month_str",
        y_=None,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
        start_col=2,
    )

    fig_auth_month = fig_upd_layout(
        fig_auth_month,
        xgrid=False,
        colorway=px.colors.qualitative.Pastel1,
        legend=dict(
            orientation="v",
            xanchor="right",
            title_text="Banning Authority",
            title_font=dict(size=12),
            font=dict(size=10),
        ),
    )

    # For the category, use the pivoted dataframe directly
    # Format the date for display
    df_cat = dmt_by_category_over_time.copy()
    df_cat["month_str"] = pd.to_datetime(df_cat["month"]).dt.strftime("%Y-%m")

    fig_cat_month = generate_stacked_bar(
        title="Content Categories Targeted for Blocking",
        subtitle="Monthly distribution of blocked websites by content type",
        df=df_cat,
        x_="month_str",
        y_=None,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
        start_col=2,
    )

    fig_cat_month = fig_upd_layout(
        fig_cat_month,
        xgrid=False,
        colorway=px.colors.qualitative.Alphabet_r,
        legend=dict(
            orientation="v",
            xanchor="right",
            title_text="Content Category",
            title_font=dict(size=12),
            font=dict(size=10),
        ),
    )

    return (fig_auth_month, fig_cat_month)


@callback(
    [
        Output("pg_block_fig_country_month", "figure"),
        Input("pg_block_fig_country_month", "id"),
    ],
)
def group_8(f1):
    #########################################
    #########################################
    # Block sites by month, by country domain
    # Stacked bar chart for country distribution over time

    # Use the country pivoted dataframe directly
    # Format the date for display
    dmt_by_country_over_time["month_str"] = pd.to_datetime(
        dmt_by_country_over_time["month"]
    ).dt.strftime("%Y-%m")

    fig_country_month = generate_stacked_bar(
        title="Website Blocking Trends by Country",
        subtitle="Monthly distribution of blocked domains by country (top 25)",
        df=dmt_by_country_over_time,
        x_="month_str",
        y_=None,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
        start_col=2,
    )

    fig_country_month = fig_upd_layout(
        fig_country_month,
        xgrid=False,
        colorway=px.colors.qualitative.Pastel1,
    )

    return [fig_country_month]


@callback(
    [
        Output("pg_block_fig_sankey", "figure"),
        Input("pg_block_fig_sankey", "id"),
    ],
)
def group_9(f1):
    #########################################
    #########################################
    # Relation between authority and category
    # Sankey diagram
    color_src = {0: "#3c116f", 1: "#7d2470", 2: "#be3954", 3: "#ff9f6f"}
    color_label = {
        "Office of the Prosecutor General": "#be3954",
        "Minstry of Foreign Affairs": "#3c116f",
        "Not specified": "#7d2470",
        "Roskomnadzor": "#ff9f6f",
    }

    fig_sankey = go.Figure(
        go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
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
    )
    fig_sankey = fig_upd_layout(
        fig_sankey,
        title="Mapping Censorship: Authorities and Targeted Content Categories",
        subtitle="Relationship between Russian authorities and the types of websites they block",
    )
    fig_sankey.update_layout(height=600)

    return [fig_sankey]


@callback(
    [
        Output("pg_block_fig_treemap", "figure"),
        Input("pg_block_fig_treemap", "id"),
    ],
)
def group_10(f1):
    #########################################
    #########################################
    # Top 6 countries by category
    # Treemap

    # Get unique countries from the dataframe
    unique_countries = dmt_by_country_category_top_6["label"].unique()

    # Get a color palette with enough colors
    color_palette = px.colors.qualitative.Safe + px.colors.qualitative.Vivid

    # Create colors_country dictionary dynamically
    colors_country = {}
    for i, country in enumerate(unique_countries):
        # Assign color from palette (cycling if needed)
        colors_country[country] = color_palette[i % len(color_palette)]

    # Create the treemap
    fig_treemap = generate_treemap(
        title="Top Blocked Website Categories by Country in Russia",
        subtitle="",
        df=dmt_by_country_category_top_6,
        colors=colors_country,
    )

    return [fig_treemap]


@callback(
    [
        Output("pg_block_fig_bar_country_cat", "figure"),
        Input("pg_block_fig_bar_country_cat", "id"),
    ],
)
def group_11(f1):
    #########################################
    #########################################
    # Top 6+ countries by category
    # Stacked bar chart

    fig_bar_country_cat = generate_stacked_bar(
        title="Blocked Websites by Country and Category",
        subtitle="",
        df=dmt_by_country_category_after_6,
        x_="country_domain",
        y_=None,
        hovertemplate="%{x}<br>%{y} blocked websites<extra></extra>",
    )

    # Update layout for stacked bar chart
    fig_bar_country_cat = fig_upd_layout(
        fig_bar_country_cat,
        xgrid=False,
        legend_title="Category",
    )

    return [fig_bar_country_cat]
