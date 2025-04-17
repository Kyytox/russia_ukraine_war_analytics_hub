import pandas as pd
import dash
from dash import Dash, Input, Output, callback, html, dcc
import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.colors as pc

from assets.components.warning_sources import warning_sources
from assets.components.figure_chart import box_chart


from utils.variables import PATH_DMT_COMPO_WEAPONS
from utils.variables_charts import (
    COLORS_COMP_WEAPONS,
)

from utils.utils_charts import fig_upd_layout
from utils.generate_chart import (
    generate_treemap,
    generate_sunburst,
    generate_bar,
    generate_pie,
    generate_stacked_bar,
)

dash.register_page(__name__)


dmt_global = pd.read_parquet(f"{PATH_DMT_COMPO_WEAPONS}/dmt_global_count.parquet")
dmt_treemap_country_weapon_type = pd.read_parquet(
    f"{PATH_DMT_COMPO_WEAPONS}/dmt_treemap_country_weapon_type.parquet"
)
dmt_sunburst_equipments_weapon_type = pd.read_parquet(
    f"{PATH_DMT_COMPO_WEAPONS}/dmt_sunburst_equipments_weapon_type.parquet"
)
dmt_country_by_weapon = pd.read_parquet(
    f"{PATH_DMT_COMPO_WEAPONS}/dmt_country_by_weapon.parquet"
)


markdown_text = """
The data has been collected from **[War & Sanctions - Сomponents in the aggressor`s weapon](https://war-sanctions.gur.gov.ua/en/components)**

This page is a graphical representation of the data.
"""

# Layout
layout = html.Div(
    className="page-content",
    children=[
        # Header
        html.H1(
            className="page-title",
            children="⚙️ Сomponents in Aggressor`s Weapon",
        ),
        warning_sources(markdown_text),
        html.Div(
            className="div-group-chart",
            children=[
                # Metrics global
                html.Div(
                    className="div-group-metrics",
                    children=[
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3(
                                    f"{dmt_global.query('libelle == "weapon"')['count'].sum():.0f}",
                                ),
                                html.P("Weapon Components"),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3("USA"),
                                html.P("Country with the most components"),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3("Analog Devices"),
                                html.P("Biggest Manufacturer"),
                            ],
                        ),
                        html.Div(
                            className="div-metrics",
                            children=[
                                html.H3("Shahed-136 UAV"),
                                html.P("Weapon with the most components"),
                            ],
                        ),
                    ],
                    style={"width": "13%"},
                ),
                box_chart("pg_weap_fig1", {}, "41%", "60vh"),
                box_chart("pg_weap_fig2", {}, "41%", "60vh"),
            ],
        ),
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_weap_fig3", {}, "48.5%", "85vh"),
                box_chart("pg_weap_fig4", {}, "48.5%", "85vh"),
            ],
        ),
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_weap_fig5", {}, "98%", "125vh"),
            ],
        ),
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_weap_fig8", {}, "98%", "125vh"),
            ],
        ),
        html.Div(
            className="div-group-chart",
            children=[
                box_chart("pg_weap_fig6", {}, "57%", "80vh"),
                box_chart("pg_weap_fig7", {}, "40%", "63vh"),
            ],
        ),
    ],
)


# Callback
@callback(
    [
        Output("pg_weap_fig1", "figure"),
        Output("pg_weap_fig2", "figure"),
        Input("pg_weap_fig1", "id"),
        Input("pg_weap_fig2", "id"),
    ],
)
def group_1(f1, f2):

    # #########################################
    # #########################################
    # Components by equipment type
    # Pie chart
    df = dmt_global[dmt_global["libelle"] == "type_equipment"]
    fig1 = generate_pie(
        title="Weapon Components by Equipment Type",
        subtitle="Number of components found in the aggressor's weapons by equipment type",
        labels=df["metric"],
        values=df["count"],
        center_txt=f"{df['count'].sum().astype(int)}<br>Components",
        dict_colors=COLORS_COMP_WEAPONS,
        hovertemplate="%{label}<br>%{value} Components<extra></extra>",
    )

    # #########################################
    # #########################################
    # Components by weapon type
    # Bar chart
    df = dmt_global[dmt_global["libelle"] == "weapon_type"].sort_values(
        by="count", ascending=False
    )
    fig2 = generate_bar(
        title="Weapon Components by Weapon Type",
        subtitle="Number of components found in the aggressor's weapons by weapon type",
        x_=df["metric"],
        y_=df["count"],
        text=df["count"],
        colors="#196481",
        hoverinfo="y+text",
        hovertemplate="%{x}<br>%{y} Components<extra></extra>",
    )
    fig2 = fig_upd_layout(
        fig2,
        xgrid=False,
        xaxis_title="Weapon Type",
        yaxis_title="Number of Components",
    )
    return (fig1, fig2)


@callback(
    [
        Output("pg_weap_fig3", "figure"),
        Output("pg_weap_fig4", "figure"),
        Input("pg_weap_fig3", "id"),
        Input("pg_weap_fig4", "id"),
    ],
)
def group_2(f1, f2):
    # #########################################
    # #########################################
    # Components by Manufacturer country
    # Bar chart
    df = dmt_global[dmt_global["libelle"] == "manufacturer_country"]
    df = df.sort_values(by="count", ascending=True)

    fig3 = generate_bar(
        title="Weapon Components by Manufacturer Country",
        subtitle="Number of components found in the aggressor's weapons by manufacturer country",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#817f19",
        orientation="h",
        hoverinfo="y+text",
        hovertemplate="%{x}<br>%{y} Components<extra></extra>",
    )
    fig3 = fig_upd_layout(
        fig3,
        ygrid=False,
        xaxis_title="Number of Components",
    )

    # #########################################
    # #########################################
    # Components by Manufacturer
    # Bar chart
    df = dmt_global[dmt_global["libelle"] == "manufacturer"].head(30)
    df = df.sort_values(by="count", ascending=True)
    fig4 = generate_bar(
        title="Weapon Components by Manufacturer (Top 30)",
        subtitle="Number of components found in the aggressor's weapons by manufacturer (Top 30)",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#19815e",
        orientation="h",
        hovertemplate="%{y}<br>%{x} Components<extra></extra>",
    )
    fig4 = fig_upd_layout(
        fig4,
        ygrid=False,
        xaxis_title="Number of Components",
    )
    return (fig3, fig4)


@callback(
    [
        Output("pg_weap_fig5", "figure"),
        Output("pg_weap_fig8", "figure"),
        Input("pg_weap_fig5", "id"),
        Input("pg_weap_fig8", "id"),
    ],
)
def group_3(f1, f2):

    # #########################################
    # #########################################
    # Components by Weapon
    # Bar chart
    df = dmt_global[dmt_global["libelle"] == "weapon"].head(60)
    df = df.sort_values(by="count", ascending=True)
    df["metric"] = df["metric"].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

    fig5 = generate_bar(
        title="Components Found in Weapon (Top 60)",
        subtitle="Number of components found in the aggressor's weapons by weapon (Top 60)",
        x_=df["count"],
        y_=df["metric"],
        text=df["count"],
        colors="#8f1414",
        orientation="h",
        hovertemplate="%{y}<br>%{x} Components<extra></extra>",
    )
    fig5 = fig_upd_layout(
        fig5,
        ygrid=False,
        xaxis_title="Number of Components",
        yaxis_title="Weapons",
    )

    # ###########################################
    # ###########################################
    # Sunburst by Equipment Type and Weapon Type
    # Sunburst
    df = dmt_country_by_weapon

    # Get only 50 first caracters
    df["weapon"] = df["weapon"].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

    # Sort the weapons by count
    weapons_sorted = (
        df.groupby("weapon")["count"].sum().sort_values(ascending=True).index.tolist()
    )

    # Pivot
    df_pivot = df.pivot_table(
        index="weapon",
        columns="manufacturer_country",
        values="count",
        aggfunc="sum",
        fill_value=0,
    )
    df_pivot = df_pivot.reindex(weapons_sorted)
    df_pivot = df_pivot.reset_index()

    fig8 = generate_stacked_bar(
        title="Weapon Components by Manufacturer Country and Weapon",
        subtitle="Number of components found in the aggressor's weapons by manufacturer country and weapon",
        df=df_pivot,
        x_=None,
        y_="weapon",
        orientation="h",
    )

    fig8 = fig_upd_layout(
        fig8,
        ygrid=False,
        xaxis_title="Number of Components",
        yaxis_title="Weapons",
        colorway=pc.sequential.Plasma + pc.sequential.OrRd + pc.sequential.Viridis_r,
    )
    return (fig5, fig8)


@callback(
    [
        Output("pg_weap_fig6", "figure"),
        Output("pg_weap_fig7", "figure"),
        Input("pg_weap_fig6", "id"),
        Input("pg_weap_fig7", "id"),
    ],
)
def group_4(f1, f2):
    # ###########################################
    # ###########################################
    # Components by Weapon
    # Stacked Bar chart
    fig6 = generate_treemap(
        title="Weapon Components by Manufacturer Country and Weapon Type (Top 10 Countries)",
        subtitle="Number of components found in the aggressor's weapons by manufacturer country and weapon type for the top 10 countries",
        df=dmt_treemap_country_weapon_type,
        colors=COLORS_COMP_WEAPONS,
    )

    # #########################################
    # #########################################
    # TreeMap by Country and Weapon Type
    # TreeMap
    fig7 = generate_sunburst(
        title="Weapon Components by Equipment Type and Weapon Type",
        subtitle="Number of components found in the aggressor's weapons by equipment type and weapon type",
        df=dmt_sunburst_equipments_weapon_type,
        colors=COLORS_COMP_WEAPONS,
    )

    return (fig6, fig7)
