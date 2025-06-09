import json
import pandas as pd
import numpy as np

import dash
from dash import Dash, dcc, html, Input, Output, callback
import dash_daq as daq

import plotly.graph_objects as go

from utils.variables import PATH_DMT_INC_RAILWAY
from utils.variables_charts import COLORS_RAILWAY

from assets.components.warning_sources import warning_sources
from assets.components.figure_chart import box_chart


from utils.utils_charts import fig_upd_layout
from utils.generate_chart import (
    prepare_df_region,
    generate_pie,
    generate_bar,
    generate_line,
    generate_heatmap,
    generate_treemap,
    generate_treemap2,
    generate_sunburst,
    generate_stacked_bar,
    generate_map,
    generate_wordcloud,
    generate_funnel,
    generate_sankey,
    generate_waterfall,
)

dash.register_page(
    __name__,
    external_stylesheets=[],
)

sufix_subtitle = "on the Russian Railways Network from 2022 to 2024"


# read file json
with open("core/utils/ru_region.json") as file:
    polygons = json.load(file)


dmt_inc_total = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_total.parquet")
dmt_inc_year = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_by_year.parquet")
dmt_inc_month = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_by_month.parquet")
dmt_inc_cumul_month = pd.read_parquet(
    f"{PATH_DMT_INC_RAILWAY}/inc_cumul_by_month.parquet"
)
dmt_inc_region = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_by_region.parquet")
dmt_inc_day_week = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_by_day_week.parquet")
dmt_inc_dmg_eqp = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/inc_type_dmg_eqp.parquet")
dmt_sun_tree = pd.read_parquet(
    f"{PATH_DMT_INC_RAILWAY}/inc_type_dmg_eqp_sun_tree.parquet"
)
dmt_app_laws_prtsn_age = pd.read_parquet(
    f"{PATH_DMT_INC_RAILWAY}/app_laws_prtsn_age.parquet"
)
dmt_wordcloud = pd.read_parquet(f"{PATH_DMT_INC_RAILWAY}/wordcloud.parquet")
dmt_sab_prtsn_grp = pd.read_parquet(
    f"{PATH_DMT_INC_RAILWAY}/sabotage_by_prtsn_grp.parquet"
)

# -----------LISTS-----------
lst_inc_type = dmt_inc_total[dmt_inc_total["type"] == "inc_type"]["label"].unique()
lst_dmg_eqp = dmt_inc_total[dmt_inc_total["type"] == "dmg_eqp"]["label"].unique()
lst_sabotage_dmg_eqp = dmt_inc_total[dmt_inc_total["type"] == "sabotage"][
    "label"
].unique()
list_partisans_group = dmt_inc_total[dmt_inc_total["type"] == "prtsn_grp"][
    "label"
].unique()

# -----------DF CHARTS-----------

df_incd_typ_total = dmt_inc_total[dmt_inc_total["type"] == "inc_type"]
df_dmg_eqp_total = dmt_inc_total[dmt_inc_total["type"] == "dmg_eqp"]
df_incd_reg_total = dmt_inc_total[dmt_inc_total["type"] == "region"]
df_inc_type_cumul_month = dmt_inc_cumul_month[dmt_inc_cumul_month["inc_type"].notnull()]
df_dmg_eqp_cumul_month = dmt_inc_cumul_month[dmt_inc_cumul_month["dmg_eqp"].notnull()]

#
df_nb_inc_year = (
    dmt_inc_year[dmt_inc_year["label"] == "Total"]
    .drop(columns="type")
    .melt(id_vars=["label"], var_name="year", value_name="total_inc")
)

#
df_nb_inc_month_line = (
    dmt_inc_month[
        (
            dmt_inc_month["inc_type"].isnull()
            & dmt_inc_month["dmg_eqp"].isnull()
            & dmt_inc_month["coll_with"].isnull()
        )
    ]
    .sort_values("year")
    .pivot(index="month", columns="year", values="total_inc")
    .reset_index()
)

#
df_nb_inc_month_bar = dmt_inc_month[
    (
        dmt_inc_month["inc_type"].isnull()
        & dmt_inc_month["dmg_eqp"].isnull()
        & dmt_inc_month["coll_with"].isnull()
    )
]

#
df_inc_type_cumul = df_inc_type_cumul_month.pivot(
    index="month_year", columns="inc_type", values="cumul_inc"
).reset_index()

#
df_inc_type_year = dmt_inc_year[(dmt_inc_year["type"] == "inc_type")]

#
df_inc_type_month = (
    dmt_inc_month[
        (
            dmt_inc_month["inc_type"].notnull()
            & dmt_inc_month["dmg_eqp"].isnull()
            & dmt_inc_month["coll_with"].isnull()
        )
    ]
    .pivot(index="month_year", columns="inc_type", values="total_inc")
    .reset_index()
)

#
df_sun_tree = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "inc_type") & (dmt_sun_tree["chart"] == "sun_tree")
]

df_map_inc_reg = df_incd_reg_total.copy()
df_map_inc_reg = prepare_df_region(df_map_inc_reg)

#
df_sankey = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "inc_type") & (dmt_sun_tree["chart"] == "sankey")
].reset_index(drop=True)

#

df_inc_type_region = dmt_inc_region[["region"] + list(lst_inc_type)]
df_inc_type_region["region"] = pd.Categorical(
    df_inc_type_region["region"],
    categories=df_incd_reg_total["label"],
    ordered=True,
)
df_inc_type_region = df_inc_type_region.sort_values("region", ascending=False)

# TAB 3
# Damaged Equipments
df_inc_inc_type = df_dmg_eqp_cumul_month.pivot(
    index="month_year", columns="dmg_eqp", values="cumul_inc"
).reset_index()

# damaged equipment by year
df_inc_dmg_eqp_year = dmt_inc_year[(dmt_inc_year["type"] == "dmg_eqp")]

# damaged equipment by month
df_inc_dmg_eqp_month = (
    dmt_inc_month[
        (dmt_inc_month["inc_type"].isnull() & dmt_inc_month["dmg_eqp"].notnull())
    ]
    .pivot(
        index="month_year",
        columns="dmg_eqp",
        values="total_inc",
    )
    .reset_index()
)

# sunburst chart
df_sunburst_tab3 = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "dmg_eqp") & (dmt_sun_tree["chart"] == "sun_tree")
]

# sankey diagram
df_sankey_tab3 = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "dmg_eqp") & (dmt_sun_tree["chart"] == "sankey")
].reset_index(drop=True)

# dmg equip by region
df_dmg_eqp_region = dmt_inc_region[["region"] + list(lst_dmg_eqp)]
df_dmg_eqp_region["region"] = pd.Categorical(
    df_dmg_eqp_region["region"], categories=df_incd_reg_total["label"], ordered=True
)
df_dmg_eqp_region["total"] = df_dmg_eqp_region.drop(columns="region").sum(axis=1)
df_dmg_eqp_region = df_dmg_eqp_region.sort_values("total", ascending=True).drop(
    columns="total"
)

# TAB 4
# Pie chart
df_coll_pie = dmt_inc_total[dmt_inc_total["type"] == "coll_with"]

# Collisions by month
df_coll_month = (
    dmt_inc_month[
        (dmt_inc_month["inc_type"].isnull())
        & (dmt_inc_month["dmg_eqp"].isnull())
        & (dmt_inc_month["coll_with"].notnull())
    ]
    .pivot(index="month_year", columns="coll_with", values="total_inc")
    .reset_index()
)

# Collisions by damaged equipment
df_col_dmg_eqp = dmt_inc_total[dmt_inc_total["type"] == "coll_eqp"]

# Collisions by damaged equipment by month
df_col_dmg_eqp_month = (
    dmt_inc_month[
        (dmt_inc_month["inc_type"] == "Collision")
        & (dmt_inc_month["dmg_eqp"].notnull())
        & (dmt_inc_month["coll_with"].isnull())
    ]
    .pivot(index="month_year", columns="dmg_eqp", values="total_inc")
    .reset_index()
)

# TAB 5
# Sabotage by year
df_sab_year = (
    dmt_inc_year[(dmt_inc_year["label"] == "Sabotage")]
    .drop(columns="type")
    .melt(id_vars=["label"], var_name="year", value_name="total_inc")
)

# Sabotage by month
df_sab_month = (
    dmt_inc_month[
        (dmt_inc_month["inc_type"] == "Sabotage")
        & (dmt_inc_month["dmg_eqp"].isnull() & dmt_inc_month["coll_with"].isnull())
    ]
    .pivot(index="month", columns="year", values="total_inc")
    .reset_index()
)

# Comparison between incidents and sabotage
df_comp_sab_inc = (
    dmt_inc_month[
        (
            (dmt_inc_month["inc_type"] == "Sabotage")
            & (dmt_inc_month["dmg_eqp"].isnull())
            & (dmt_inc_month["coll_with"].isnull())
        )
        | (
            (dmt_inc_month["inc_type"].isnull())
            & (dmt_inc_month["dmg_eqp"].isnull())
            & (dmt_inc_month["coll_with"].isnull())
        )
    ]
    .fillna("Total")
    .pivot(index="month_year", columns="inc_type", values="total_inc")
    .reset_index()
)
df_comp_sab_inc = df_comp_sab_inc[["month_year", "Total", "Sabotage"]]

# Sabotage by Partisans Group
df_sab_prtsn_grp = dmt_inc_total[dmt_inc_total["type"] == "prtsn_grp"]

# Sabotage by Damaged Equipment by Partisans Group
df_dmg_eqp_prtsn_grp = (
    dmt_inc_total[dmt_inc_total["type"].isin(list_partisans_group)]
    .pivot(index="label", columns="type", values="total_inc")
    .reset_index()
)

# Tree map Sabotage Partisans Group by Damaged Equipment
df_tree_prtsn_eqp = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "dmg_eqp_prtsn_grp") & (dmt_sun_tree["chart"] == "sun_tree")
]

# Tree map Sabotage Damaged Equipment by Partisans Group
df_tree_eqp_prtsn = dmt_sun_tree[
    (dmt_sun_tree["tab"] == "prtsn_grp_dmg_eqp") & (dmt_sun_tree["chart"] == "sun_tree")
]

# Bar Sabotage damaged equipment by Partisans Group
df_bar_eqp_prtsn = (
    dmt_inc_total[dmt_inc_total["type"].isin(list_partisans_group)]
    .pivot(index="type", columns="label", values="total_inc")
    .reset_index()
)

# Bar Sabotage by Damaged Equipment
df_pie_sab_eqp = dmt_inc_total[dmt_inc_total["type"] == "sabotage"]

# Bar Sabotage by Damaged Equipment by Month
df_bar_sab_eqp_month = (
    dmt_inc_month[
        (dmt_inc_month["inc_type"] == "Sabotage")
        & (dmt_inc_month["dmg_eqp"].notnull())
        & (dmt_inc_month["coll_with"].isnull())
    ]
    .pivot(
        index="month_year",
        columns="dmg_eqp",
        values="total_inc",
    )
    .reset_index()
)

# Bar Sabotage by Damaged Equipment by Region
df_sab_eqp_region = dmt_inc_region[
    ["region"] + [col for col in dmt_inc_region.columns if col.startswith("sab_")]
]
# remove sab_
df_sab_eqp_region.columns = df_sab_eqp_region.columns.str.replace("sab_", "")
df_sab_eqp_region["region"] = pd.Categorical(
    df_sab_eqp_region["region"], categories=df_incd_reg_total["label"], ordered=True
)

# remove rows where all 0 except region
df_sab_eqp_region = df_sab_eqp_region[
    (df_sab_eqp_region.drop(columns="region") != 0).any(axis=1)
]

df_sab_eqp_region = df_sab_eqp_region.sort_values("region", ascending=False)

df_map_sab_reg = dmt_inc_total[dmt_inc_total["type"] == "sab_region"]
df_map_sab_reg = prepare_df_region(df_map_sab_reg)

# TAB 6
# Funnel
df_funnel = pd.DataFrame(
    {
        "niv": ["Sabotage", "Sabotage with Arrest", "Total Arrested"],
        "count": [
            dmt_inc_total[dmt_inc_total["label"] == "Sabotage"]["total_inc"].values[0],
            dmt_inc_total[dmt_inc_total["label"] == "Sabotage with Arrest"][
                "total_inc"
            ].values[0],
            dmt_inc_total[dmt_inc_total["label"] == "Total Arrested"][
                "total_inc"
            ].values[0],
        ],
    }
)

# Nb Partisans Arrested
df_prtsn_arr = dmt_inc_total[dmt_inc_total["type"] == "prtsn_arr"]

# Nb App Laws
df_app_laws = dmt_inc_total[dmt_inc_total["type"] == "app_laws"]

# Waffle chart
df_waffle = dmt_inc_total[
    (dmt_inc_total["type"] == "prtsn_age")
    & (dmt_inc_total["label"] != "Mean")
    & (~dmt_inc_total["label"].str.startswith("age"))
]

# Waterfall chart
df_waterfall = dmt_inc_total[
    (dmt_inc_total["type"] == "prtsn_age")
    & (dmt_inc_total["label"].str.startswith("age"))
]
df_waterfall["label"] = df_waterfall["label"].str.split("_").str[1]
df_waterfall = df_waterfall.sort_values("label")
df_waterfall = df_waterfall[df_waterfall["label"] != "0"]  # remove age 0


def tab_overview():

    # #########################################
    # #########################################
    # Wordcloud
    # fig0 = create_wordcloud(df=dmt_wordcloud)

    # #########################################
    # #########################################
    # Number of Incidents by Year
    # Line Chart
    fig1 = generate_bar(
        title="Annual Railway Incidents in Russia",
        subtitle=f"Number of Incidents {sufix_subtitle}",
        x_=df_nb_inc_year["year"],
        y_=df_nb_inc_year["total_inc"],
        text=df_nb_inc_year["total_inc"],
        colors=COLORS_RAILWAY,
    )
    fig1 = fig_upd_layout(fig1, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Month
    # Line Chart
    df_nb_inc_month_line = (
        dmt_inc_month[
            (
                dmt_inc_month["inc_type"].isnull()
                & dmt_inc_month["dmg_eqp"].isnull()
                & dmt_inc_month["coll_with"].isnull()
            )
        ]
        .sort_values("year")
        .pivot(index="month", columns="year", values="total_inc")
        .reset_index()
    )

    df_nb_inc_month_line = df_nb_inc_month_line[["month", 2025, 2024, 2023, 2022]]
    fig2 = generate_line(
        title="Monthly Incident Trends on Russian Railways",
        subtitle=f"Number of Incidents by Month {sufix_subtitle}",
        df=df_nb_inc_month_line,
        col_x="month",
        fill=None,
    )
    fig2 = fig_upd_layout(fig2, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Month
    # Bar Chart
    df_nb_inc_month_bar = dmt_inc_month[
        (
            dmt_inc_month["inc_type"].isnull()
            & dmt_inc_month["dmg_eqp"].isnull()
            & dmt_inc_month["coll_with"].isnull()
        )
    ]
    fig3 = generate_bar(
        title="Monthly Incident Trends on Russian Railways",
        subtitle=f"Number of Incidents by Month {sufix_subtitle}",
        x_=df_nb_inc_month_bar["month_year"],
        y_=df_nb_inc_month_bar["total_inc"],
        text=df_nb_inc_month_bar["total_inc"],
        colors=COLORS_RAILWAY,
    )

    fig3 = fig_upd_layout(fig3, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents Types
    # Bar Chart
    fig4 = generate_bar(
        title="Railway Incidents by Type in Russia",
        subtitle=f"Number of reported incidents categorized by type<br>{sufix_subtitle}",
        x_=df_incd_typ_total["label"],
        y_=df_incd_typ_total["total_inc"],
        text=df_incd_typ_total["total_inc"],
        colors=COLORS_RAILWAY,
    )
    fig4 = fig_upd_layout(fig4, xgrid=False)

    # #########################################
    # #########################################
    # Number of Damaged Equipments
    # Bar Chart
    fig5 = generate_bar(
        title="Railway Damaged Equipments in Russia",
        subtitle=f"Number of reported Damaged Equipments<br>{sufix_subtitle}<br>(without Collisions with Humans)",
        x_=df_dmg_eqp_total["label"],
        y_=df_dmg_eqp_total["total_inc"],
        text=df_dmg_eqp_total["total_inc"],
        colors=COLORS_RAILWAY,
    )
    fig5 = fig_upd_layout(fig5, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Region
    # Treemap
    fig6 = generate_treemap2(
        title="Distribution of Railway Incidents by Region in Russia",
        subtitle="",
        ids_=df_incd_reg_total["label"],
        labels_=df_incd_reg_total["label"],
        parents_=[""] * len(df_incd_reg_total),
        values_=df_incd_reg_total["total_inc"],
        colors=df_incd_reg_total["total_inc"],
    )

    # #########################################
    # #########################################
    # Number of Incidents by Region
    # Map
    fig7 = generate_map(
        title="Geographic Distribution of Sabotages on Russian Railways",
        subtitle="Number of Railway Sabotages by Region in Russia from 2023 to 2024",
        polygons=polygons,
        loc=df_map_inc_reg["id_region"],
        text_=df_map_inc_reg["region"],
        z_=df_map_inc_reg["total_inc"],
        locationmode="geojson-id",
    )

    fig7 = fig_upd_layout(
        fig7,
        geo=dict(
            projection=dict(type="eckert1", scale=3.4),
            center=dict(lat=62, lon=98),
        ),
        dragmode=False,
    )

    # #########################################
    # #########################################
    # Number of Incidents by Day
    # Heatmap
    list_years = dmt_inc_day_week.columns.str.split("_").str[0].unique()

    heats = []

    for year in list_years:
        # get data where columns as year
        df_year = dmt_inc_day_week[
            dmt_inc_day_week.columns[dmt_inc_day_week.columns.str.startswith(year)]
        ]

        # remove year from columns
        df_year.columns = df_year.columns.str.split("_").str[1]

        fig_heat = generate_heatmap(
            title=f"Distribution of Railway Incidents by Day of the Week in {year}",
            subtitle=f"Number of incidents categorized by day of the week {sufix_subtitle}",
            x_=df_year.columns,
            y_=df_year.index,
            z_=df_year,
        )
        fig_heat = fig_upd_layout(
            fig_heat,
            xaxis_title="Weeks",
            xaxis_tickvals=np.arange(0, 52, 1),
        )
        heats.append(fig_heat)

    security_data = []
    for _, row in dmt_wordcloud.iterrows():
        text = row.iloc[0]  # First column (text or label)
        value = row.iloc[1]  # Second column (frequency or value)
        security_data.append([text, value])

    markdown_text = """
    Warning: Rail incidents in **Crimea** have been accounted for., it is good to remember, **Crimea** is Ukrainian, but being occupied the rail network is under the responsibility of the Orcs.
    """

    return html.Div(
        children=[
            html.H2("Overview"),
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
                                        f"{int(dmt_inc_total[dmt_inc_total['label'] == 'Total']['total_inc'].values[0])}"
                                    ),
                                    html.P("Total Incidents"),
                                ],
                            ),
                            html.Div(
                                className="div-metrics",
                                children=[
                                    html.H3(
                                        f"{int(dmt_inc_year['2024'][dmt_inc_year['label'] == 'Total'].values[0])}"
                                    ),
                                    html.P(
                                        f"+{int(dmt_inc_year['2024'][dmt_inc_year['label'] == 'Total'].values[0] - dmt_inc_year['2023'][dmt_inc_year['label'] == 'Total'].values[0])} from 2023"
                                    ),
                                ],
                            ),
                            html.Div(
                                className="div-metrics",
                                children=[
                                    html.H3(
                                        f"{int(dmt_inc_total[dmt_inc_total['label'] == 'Sabotage']['total_inc'].values[0])}"
                                    ),
                                    html.P("Total Number of Sabotage"),
                                ],
                            ),
                            html.Div(
                                className="div-metrics",
                                children=[
                                    html.H3(
                                        f"{int(dmt_inc_total[dmt_inc_total['label'] == 'Total Arrested']['total_inc'].values[0])}"
                                    ),
                                    html.P("Arrested Partisans"),
                                ],
                            ),
                        ],
                        style={"width": "14%"},
                    ),
                    box_chart("rail_1_fig1", fig1, "30%", "58vh"),
                    box_chart("rail_1_fig3", fig3, "53%", "58vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_1_fig2", fig2, "49%", "65vh"),
                    box_chart("rail_1_fig4", fig4, "24%", "65vh"),
                    box_chart("rail_1_fig5", fig5, "24%", "65vh"),
                ],
            ),
            html.Div(
                className="div-interact-chart",
                children=[
                    daq.BooleanSwitch(
                        id="togl_rail_1_fig6",
                        on=True,
                        className="switch",
                        label="Hide Moscow",
                        color="#ea7d00",
                    ),
                    box_chart("rail_1_fig6", fig6, "100%", "55vh"),
                ],
            ),
            html.Div(
                className="div-interact-chart",
                children=[
                    daq.BooleanSwitch(
                        id="togl_rail_1_fig7",
                        on=True,
                        className="switch",
                        label="Hide Moscow",
                        color="#ea7d00",
                    ),
                    box_chart("rail_1_fig7", fig7, "100%", "85vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_1_fig8", heats[0], "49%", "35vh"),
                    box_chart("rail_1_fig9", heats[1], "49%", "35vh"),
                    box_chart("rail_1_fig10", heats[2], "49%", "35vh"),
                    box_chart("rail_1_fig11", heats[3], "49%", "35vh"),
                    # we can't use for , i don't know why but if do it, the chart is displayed in Overview and Sabotage tabs
                ],
            ),
        ],
    )


def tab_incidents_types():

    # #########################################
    # #########################################
    # Number of Incidents by Incident Type
    # Pie Chart
    fig1 = generate_pie(
        title="Distribution of Incidents by Type",
        subtitle=f"Distribution of reported incidents categorized by type {sufix_subtitle}",
        labels=df_incd_typ_total["label"],
        values=df_incd_typ_total["total_inc"],
        center_txt=f"{df_incd_typ_total['total_inc'].sum().astype(int)}<br>Incidents",
        dict_colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Evolution of Incidents by Incident Type
    # Line Chart
    fig2 = generate_line(
        title="Trends in Cumulative Incidents by Type Over Time",
        subtitle=f"Cumulative number of incidents by type {sufix_subtitle}",
        df=df_inc_type_cumul,
        col_x="month_year",
        mode="lines",
        fill=None,
    )
    fig2 = fig_upd_layout(fig2, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Incident Type by Year
    # Bar Chart
    fig3 = generate_stacked_bar(
        title="Incidents by Type: Yearly Comparison",
        subtitle=f"Insight into the distribution of incident types {sufix_subtitle}",
        df=df_inc_type_year,
        x_="label",
        y_=None,
        colors=COLORS_RAILWAY,
        start_col=2,
    )
    fig3 = fig_upd_layout(fig3, xgrid=False, barmode="group")

    # #########################################
    # #########################################
    # Number of Incidents by Incident Type by Month
    # Bar Chart
    fig4 = generate_stacked_bar(
        title="Monthly Incidents by Type",
        subtitle=f"A detailed view of incidents categorized by type on a monthly {sufix_subtitle}",
        df=df_inc_type_month,
        x_="month_year",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig4 = fig_upd_layout(fig4, xgrid=False)

    # #########################################
    # #########################################
    # Sunburst chart
    fig5 = generate_sunburst(
        title="Distribution of Damaged Equipment by Incident Type",
        subtitle=f"Relationship between incident types and damaged equipment {sufix_subtitle}",
        df=df_sun_tree,
        colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Sankey diagram
    fig6 = generate_sankey(
        df=df_sankey,
        colx="inc_type",
        coly="dmg_eqp",
        title="Flow of Damaged Equipment by Incident Type",
        subtitle=f"Visual representation of how incident types lead to equipment damage {sufix_subtitle}",
        df_total=dmt_inc_dmg_eqp,
    )

    # #########################################
    # #########################################
    # Number of Incidents by Incident Type by Region
    # Bar Chart
    fig7 = generate_stacked_bar(
        title="Distribution of Incidents by Type and Region",
        subtitle=f"Incidents categorized by type and region {sufix_subtitle}",
        df=df_inc_type_region,
        x_=None,
        y_="region",
        colors=COLORS_RAILWAY,
        orientation="h",
    )
    fig7 = fig_upd_layout(fig7, ygrid=False)

    return html.Div(
        children=[
            html.H2("Incidents Types"),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_2_fig1", fig1, "40%", "55vh"),
                    box_chart("rail_2_fig2", fig2, "58%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_2_fig3", fig3, "39%", "55vh"),
                    box_chart("rail_2_fig4", fig4, "59%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_2_fig5", fig5, "39%", "70vh"),
                    box_chart("rail_2_fig6", fig6, "59%", "70vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_2_fig7", fig7, "100%", "130vh"),
                ],
            ),
        ],
    )


def tab_damaged_equipments():

    # #########################################
    # #########################################
    # Number of Incidents by Damaged Equipment
    # Pie Chart
    fig1 = generate_pie(
        title="Distribution of Damaged Equipments",
        subtitle=f"Distribution of damaged equipments {sufix_subtitle}",
        labels=df_dmg_eqp_total["label"],
        values=df_dmg_eqp_total["total_inc"],
        center_txt=f"{df_dmg_eqp_total['total_inc'].sum().astype(int)}<br>Damaged Equipments",
        dict_colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Evolution of Incidents by Incident Type
    # Line Chart
    fig2 = generate_line(
        title="Trends in Cumulative Damaged Equipments Over Time",
        subtitle=f"Cumulative number of damaged equipments {sufix_subtitle}",
        df=df_inc_inc_type,
        col_x="month_year",
        mode="lines",
        fill="none",
    )
    fig2 = fig_upd_layout(fig2, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Damaged Equipment by Year
    # Bar Chart
    fig3 = generate_stacked_bar(
        title="Damaged Equipment: Yearly Comparison",
        subtitle=f"Insight into the distribution of damaged equipment {sufix_subtitle}",
        df=df_inc_dmg_eqp_year,
        x_="label",
        y_=None,
        colors=COLORS_RAILWAY,
        start_col=2,
    )
    fig3 = fig_upd_layout(fig3, xgrid=False, barmode="group")

    # #########################################
    # #########################################
    # Number of Incidents by Damaged Equipment by Month
    # Bar Chart
    fig4 = generate_stacked_bar(
        title="Monthly Damaged Equipments",
        subtitle=f"A detailed view of damaged equipments on a monthly {sufix_subtitle}",
        df=df_inc_dmg_eqp_month,
        x_="month_year",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig4 = fig_upd_layout(fig4, xgrid=False)

    # #########################################
    # #########################################
    #  Sunburst chart
    fig5 = generate_sunburst(
        title="Distribution of Incident Types by Damaged Equipment",
        subtitle=f"Relationship between damaged equipment and incident types {sufix_subtitle}",
        df=df_sunburst_tab3,
        colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Sankey diagram
    fig6 = generate_sankey(
        df=df_sankey_tab3,
        colx="dmg_eqp",
        coly="inc_type",
        title="Flow of Incident Types by Damaged Equipment",
        subtitle=f"Visual representation of how damaged equipment leads to incident types {sufix_subtitle}",
        df_total=dmt_inc_dmg_eqp,
    )

    # #########################################
    # #########################################
    # Number of Incidents by Damaged Equipment by Region
    # Bar Chart
    fig7 = generate_stacked_bar(
        title="Distribution of Damaged Equipments by Region",
        subtitle=f"Damaged equipments categorized by region {sufix_subtitle}",
        df=df_dmg_eqp_region,
        x_=None,
        y_="region",
        colors=COLORS_RAILWAY,
        orientation="h",
    )
    fig7 = fig_upd_layout(fig7, ygrid=False)

    return html.Div(
        children=[
            html.H2("Damaged Equipments Content"),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_3_fig1", fig1, "40%", "55vh"),
                    box_chart("rail_3_fig2", fig2, "58%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_3_fig3", fig3, "39%", "55vh"),
                    box_chart("rail_3_fig4", fig4, "59%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_3_fig5", fig5, "39%", "70vh"),
                    box_chart("rail_3_fig6", fig6, "59%", "70vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_3_fig7", fig7, "100%", "130vh"),
                ],
            ),
        ],
    )


def tab_collisions():

    # #########################################
    # #########################################
    # Number of Incidents by Collision With
    # Pie Chart
    fig1 = generate_pie(
        title="Distribution of Collisions",
        subtitle=f"Distribution of collisions {sufix_subtitle}",
        labels=df_coll_pie["label"],
        values=df_coll_pie["total_inc"],
        center_txt=f"{df_coll_pie['total_inc'].sum().astype(int)}<br>Collisions",
        dict_colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Number of Incidents by Collision With by Month
    # Bar Chart
    fig2 = generate_stacked_bar(
        title="Monthly Collisions",
        subtitle=f"A detailed view of collisions on a monthly {sufix_subtitle}",
        df=df_coll_month,
        x_="month_year",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig2 = fig_upd_layout(fig2, xgrid=False)

    # #########################################
    # #########################################
    # Number of Incidents by Collision With by Damaged Equipment
    # Pie Chart
    fig3 = generate_pie(
        title="Distribution of Collisions by Implicated Equipment",
        subtitle=f"Distribution of collisions by implicated equipment {sufix_subtitle}",
        labels=df_col_dmg_eqp["label"],
        values=df_col_dmg_eqp["total_inc"],
        center_txt=f"{df_col_dmg_eqp['total_inc'].sum().astype(int)}<br>Implicated Equipments",
        dict_colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Number of Damaged Equipment by Collision With by Month
    # Bar Chart
    fig4 = generate_stacked_bar(
        title="Monthly Implicated Equipments in Collisions",
        subtitle=f"A detailed view of implicated equipments in collisions on a monthly {sufix_subtitle}",
        df=df_col_dmg_eqp_month,
        x_="month_year",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig4 = fig_upd_layout(fig4, xgrid=False)

    return html.Div(
        children=[
            html.H2("Collisions Content"),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_4_fig1", fig1, "39%", "55vh"),
                    box_chart("rail_4_fig2", fig2, "59%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_4_fig3", fig3, "39%", "55vh"),
                    box_chart("rail_4_fig4", fig4, "59%", "55vh"),
                ],
            ),
        ],
    )


def tab_sabotage():

    # #########################################
    # #########################################
    # Number of Sabotage by Year
    # Bar Chart
    fig1 = generate_bar(
        title="Annual Number of Sabotage",
        subtitle=f"Number of Sabotage {sufix_subtitle}",
        x_=df_sab_year["year"],
        y_=df_sab_year["total_inc"],
        text=df_sab_year["total_inc"],
        colors=COLORS_RAILWAY,
    )
    fig1 = fig_upd_layout(fig1, xgrid=False)

    # #########################################
    # #########################################
    # Number of Sabotage by Month
    # Line Chart
    fig2 = generate_line(
        title="Monthly Number of Sabotage by Year",
        subtitle=f"Number of Sabotage {sufix_subtitle}",
        df=df_sab_month,
        col_x="month",
        fill=None,
    )
    fig2 = fig_upd_layout(fig2, xgrid=False)

    # #########################################
    # #########################################
    # Number of Sabotage by Year, compared to the total number of incidents
    # Line Chart
    fig3 = generate_line(
        title="Incidents vs Sabotage over Time",
        subtitle=f"Evolution of the number of incidents and sabotage {sufix_subtitle}",
        df=df_comp_sab_inc,
        col_x="month_year",
    )
    fig3 = fig_upd_layout(fig3, xgrid=False)

    # #########################################
    # #########################################
    # Distribution of Sabotage by Partisans Group
    # Pie Chart
    fig4 = generate_pie(
        title="Distribution of Sabotage by Partisans Group",
        subtitle=f"Distribution of sabotage incidents by partisan groups {sufix_subtitle}",
        labels=df_sab_prtsn_grp["label"],
        values=df_sab_prtsn_grp["total_inc"],
        center_txt=f"{df_sab_prtsn_grp['total_inc'].sum().astype(int)}<br>Sabotage",
        dict_colors=COLORS_RAILWAY,
    )
    # showlegend=True,

    # update traces remove text
    fig4 = fig4.update_traces(textinfo="percent+value")

    # #########################################
    # #########################################
    # Incident by day over time by partisans group
    # Scatter Chart
    fig5 = go.Figure(
        go.Scatter(
            x=dmt_sab_prtsn_grp["date"],
            y=dmt_sab_prtsn_grp["prtsn_grp"],
            mode="markers",
            marker=dict(
                color=[
                    COLORS_RAILWAY[label] for label in dmt_sab_prtsn_grp["prtsn_grp"]
                ]
            ),
        ),
        layout=dict(
            xaxis=dict(title="Date"),
        ),
    )
    fig5 = fig_upd_layout(
        fig5,
        title="Sabotage Incidents by Partisans Group",
        subtitle=f"Sabotage incidents by partisan groups {sufix_subtitle}",
        xgrid=False,
    )

    # #########################################
    # #########################################
    # Number of Partisans Group by Damaged Equipment
    # Treemap
    fig6 = generate_treemap(
        title="Treemap of Damaged Equipment with Partisan Group Attribution",
        subtitle=f"A hierarchical representation of damaged equipment categorized by the responsible partisan groups {sufix_subtitle}",
        df=df_tree_prtsn_eqp,
        colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Number of Sabotage Damaged Equipment by Partisans Group
    # Bar Chart
    fig7 = generate_stacked_bar(
        title="Implication of Partisans Group in Damaged Equipment",
        subtitle=f"Number of damaged equipment by partisan groups {sufix_subtitle}",
        df=df_dmg_eqp_prtsn_grp,
        x_="label",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig7 = fig_upd_layout(fig7, xgrid=False)

    # #########################################
    # #########################################
    # Number of Damaged Equipment by Partisans Group
    # Treemap
    fig8 = generate_treemap(
        title="Treemap of Partisan Groups and Associated Equipment Damage",
        subtitle=f"Visualizing the relationship between partisan groups and the equipment they have damaged {sufix_subtitle}",
        df=df_tree_eqp_prtsn,
        colors=COLORS_RAILWAY,
    )

    # #########################################
    # #########################################
    # Number of Sabotage Damaged Equipment by Partisans Group
    # Bar Chart
    fig9 = generate_stacked_bar(
        title="Distribution of damaged equipment of partisan groups",
        subtitle=f"Number of damaged equipment by partisan groups {sufix_subtitle}",
        df=df_bar_eqp_prtsn,
        x_="type",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig9 = fig_upd_layout(fig9, xgrid=False)

    # #########################################
    # #########################################
    # Number of Sabotage by Damaged Equipment
    # Pie Chart
    fig10 = generate_pie(
        title="Distribution of Damaged Equipment for Sabotage Incidents",
        subtitle=f"Distribution of damaged equipment for sabotage incidents {sufix_subtitle}",
        labels=df_pie_sab_eqp["label"],
        values=df_pie_sab_eqp["total_inc"],
        center_txt=f"{df_pie_sab_eqp['total_inc'].sum().astype(int)}<br>Damaged Equipments",
        dict_colors=COLORS_RAILWAY,
    )
    fig10 = fig10.update_traces(textinfo="percent+value")

    # #########################################
    # #########################################
    # Number of Sabotage by Damaged Equipment by Month
    # Bar Chart
    fig11 = generate_stacked_bar(
        title="Monthly Number of Damaged Equipments",
        subtitle=f"A detailed view of damaged equipment for sabotage incidents on a monthly {sufix_subtitle}",
        df=df_bar_sab_eqp_month,
        x_="month_year",
        y_=None,
        colors=COLORS_RAILWAY,
    )
    fig11 = fig_upd_layout(fig11, xgrid=False)

    # #########################################
    # #########################################
    # Number of Sabotage by Damaged Equipment by Region
    # Bar Chart
    fig12 = generate_stacked_bar(
        title="Number of Damaged Equipments by Region",
        subtitle=f"Number of damaged equipment for sabotage incidents categorized by region {sufix_subtitle}",
        df=df_sab_eqp_region,
        x_=None,
        y_="region",
        colors=COLORS_RAILWAY,
        orientation="h",
    )
    fig12 = fig_upd_layout(fig12, ygrid=False)

    # #########################################
    # #########################################
    # Number of Sabotage by Region
    # Map
    fig13 = generate_map(
        title="Geographic Distribution of Sabotages on Russian Railways",
        subtitle="Number of Railway Sabotages by Region in Russia from 2023 to 2024",
        polygons=polygons,
        loc=df_map_sab_reg["id_region"],
        text_=df_map_sab_reg["region"],
        z_=df_map_sab_reg["total_inc"],
        locationmode="geojson-id",
    )
    fig13 = fig_upd_layout(
        fig13,
        geo=dict(
            projection=dict(type="eckert1", scale=3.4),
            center=dict(lat=62, lon=98),
        ),
        dragmode=False,
    )

    return html.Div(
        children=[
            html.H2("Sabotage"),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig1", fig1, "34%", "55vh"),
                    box_chart("rail_5_fig2", fig2, "64%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig3", fig3, "100%", "60vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig4", fig4, "34%", "55vh"),
                    box_chart("rail_5_fig5", fig5, "64%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig6", fig6, "55%", "60vh"),
                    box_chart("rail_5_fig7", fig7, "43%", "60vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig8", fig8, "55%", "60vh"),
                    box_chart("rail_5_fig9", fig9, "43%", "60vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig10", fig10, "37%", "55vh"),
                    box_chart("rail_5_fig11", fig11, "61%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig12", fig12, "100%", "130vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_5_fig13", fig13, "100%", "85vh"),
                ],
            ),
        ],
    )


def tab_partisans_arrest():

    # #########################################
    # #########################################
    # create funnel
    fig1 = generate_funnel(
        df_funnel,
        title="Funnel of Partisans Arrested",
        subtitle=f"Visual representation of the funnel of partisans arrested for sabotage incidents {sufix_subtitle}",
    )
    # #########################################
    # #########################################
    # Number of Partisans Arrested
    # Pie Chart
    fig2 = generate_pie(
        title="Distribution of Sabotage where partisans was arrested",
        subtitle=f"Distribution of sabotage incidents where one or more partisans was arrested {sufix_subtitle}",
        labels=df_prtsn_arr["label"],
        values=df_prtsn_arr["total_inc"],
        center_txt=f"{df_prtsn_arr['total_inc'].sum().astype(int)}<br>Sabotage",
        dict_colors=COLORS_RAILWAY,
        rotation=65,
    )
    # showlegend=True,

    # #########################################
    # #########################################
    # Number of Applicable Laws
    # Treemap
    # create column colors
    df_app_laws["color"] = [COLORS_RAILWAY[label] for label in df_app_laws["label"]]

    fig3 = generate_treemap2(
        title="Distribution of Applicable Laws",
        subtitle=f"Laws of the Criminal Code of the Russian Federation applied to the partisans arrested <br>for sabotage incidents {sufix_subtitle}",
        ids_=df_app_laws["label"],
        labels_=df_app_laws["label"],
        parents_=[""] * len(df_app_laws),
        values_=df_app_laws["total_inc"],
        colors=df_app_laws["color"],
    )

    # #########################################
    # #########################################
    # Number of Partisans Age
    # Waffle Chart
    # fig4 = create_waffle(df_waffle)

    # #########################################
    # #########################################
    # Number of Partisans Age
    # Waterfall Chart
    fig5 = generate_waterfall(
        df_waterfall,
        title="Number of Partisans Arrested by Age",
        subtitle=f"Number of partisans arrested by age for sabotage incidents {sufix_subtitle}",
    )
    fig5 = fig_upd_layout(fig5)

    # #########################################
    # #########################################
    # Number of Age Group
    # Waterfall Chart
    fig6 = generate_waterfall(
        df_waterfall,
        title="Number of Partisans Arrested by Age Group",
        subtitle=f"Number of partisans arrested by age group for sabotage incidents {sufix_subtitle}",
        is_group=True,
    )
    fig6 = fig_upd_layout(fig6)

    # #########################################
    # #########################################
    # Number of Applicable Laws by Partisans Age
    # Heatmap
    # text=dmt_app_laws_prtsn_age.values,
    # hovertemplate="age: %{y}<br>law: %{x}<br>application: %{z}",
    # yaxis=dict(title="Partisans Age", type="category"),
    # xaxis=dict(tickangle=15),
    fig7 = generate_heatmap(
        title="Applicable Laws by Partisans Age",
        subtitle=f"Criminal Code Laws Applied Across Different Partisan Age {sufix_subtitle}",
        x_=dmt_app_laws_prtsn_age.columns,
        y_=dmt_app_laws_prtsn_age.index,
        z_=dmt_app_laws_prtsn_age,
    )

    return html.Div(
        children=[
            html.H2("Partisans Arrest"),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_6_fig1", fig1, "49%", "55vh"),
                    box_chart("rail_6_fig2", fig2, "49%", "55vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_6_fig3", fig3, "100%", "55vh"),
                    # dcc.Graph(
                    #     id="fig4",
                    #     className="chart",
                    #     figure=fig4,
                    #     style={"width": "59.5%", "height": "55vh"},
                    #     responsive=True,
                    # ),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_6_fig5", fig5, "49%", "75vh"),
                    box_chart("rail_6_fig6", fig6, "49%", "75vh"),
                ],
            ),
            html.Div(
                className="div-group-chart",
                children=[
                    box_chart("rail_6_fig7", fig7, "90%", "50vh"),
                ],
            ),
        ],
    )


def tab_informations():

    # Accounts Twitter
    dict_twitter = {
        "LX": "https://x.com/LXSummer1",
        "Intelschizo": "https://x.com/Schizointel",
        "Prune60": "https://x.com/Prune602",
        "NOELreports": "https://x.com/NOELreports",
        "Igor Sushko": "https://x.com/igorsushko",
    }

    # Telegram Channels
    dict_telegram = {
        "Electrichki": "https://t.me/electrichki",
        "Astra News": "https://t.me/astrapress",
        "SHOT": "https://t.me/shot_shot/",
        "Atesh": "https://t.me/atesh_ua",
        "Legion of Freedom": "https://t.me/legionoffreedom",
        "VD Legion of Freedom": "https://t.me/VDlegionoffreedom",
        "Enews112": "https://t.me/ENews112",
        "TeleRZD": "https://t.me/telerzd",
        "Mzd Rzd": "https://t.me/mzd_rzd",
        "Magistral Kuvalda": "https://t.me/magistral_kuvalda",
        "NSKZD": "https://t.me/nskzd",
        "News ZSZD": "https://t.me/news_zszd",
        "D4msk": "https://t.me/D4msk",
        "Rospartizan": "https://t.me/rospartizan",
        "Boakom": "https://t.me/boakom",
        "RDPSRU": "https://t.me/rdpsru",
        "Ostanovi Vagony 2023": "https://t.me/ostanovi_vagony_2023",
        "Activatica": "https://t.me/activatica",
        "Russvolcorps": "https://t.me/russvolcorps",
    }

    return html.Div(
        children=[
            html.H2("Informations"),
            html.H3("Description"),
            html.Div(
                [
                    html.P(
                        [
                            "This project is a data analysis and visualization tool for incidents that I was able to find on the ",
                            html.Strong(
                                "Russian Railways Network between 2022 and 2024"
                            ),
                            ", thanks to the information provided by various open sources.",
                        ]
                    ),
                    html.P(
                        [
                            "It leverages various data sources to provide insights into different types of incidents, including sabotage, collisions, and equipment damage."
                        ]
                    ),
                    html.P(
                        [
                            "The application uses ",
                            html.A(
                                "Dash", href="https://dash.plotly.com/", target="_blank"
                            ),
                            " for the interactive web interface and ",
                            html.A(
                                "Plotly", href="https://plotly.com/", target="_blank"
                            ),
                            " for creating dynamic charts and visualizations. Users can explore the data through various tabs, each focusing on a specific aspect of the incidents, such as incident types, damaged equipment, regional distribution and partisans involved.",
                        ]
                    ),
                ]
            ),
            html.Div(style={"margin": "20px 0"}),
            html.H3("Classification"),
            html.Div(
                [
                    html.P(
                        [
                            "The incidents are classified into different categories based on the type of incident, the equipment damaged, and the partisans involved. ",
                            "The classification is done using a combination of text analysis and manual labeling.",
                        ]
                    ),
                    html.P(
                        [
                            "A ",
                            html.Strong("Sabotage"),
                            " is classified when sources mention one thing evoking sabotage, some incidents are classified, for example as ",
                            html.Strong("derailment"),
                            " or ",
                            html.Strong("fire"),
                            " beacause source doesn't clearly mention or show sabotage.",
                        ]
                    ),
                ]
            ),
            html.Hr(style={"margin": "30px 0"}),
            html.H2("Data Source"),
            html.Div(
                className="warning-box",
                children=[
                    html.P(
                        [
                            "Data sources are open and free to use.",
                            html.Br(),
                            "Just, if you use it, please credits. 🫡",
                        ]
                    )
                ],
                style={
                    "backgroundColor": "#fff3cd",
                    "color": "#856404",
                    "padding": "12px 20px",
                    "borderRadius": "5px",
                    "marginBottom": "20px",
                },
            ),
            html.A(
                children=[
                    html.Button(
                        ["📜 Source: Incidents on Russian Railways Network"],
                        style={
                            "backgroundColor": "#007bff",
                            "color": "white",
                            "border": "none",
                            "padding": "10px 15px",
                            "borderRadius": "5px",
                            "cursor": "pointer",
                            "fontSize": "16px",
                        },
                    )
                ],
                href="https://docs.google.com/spreadsheets/d/1jyD1bB0uauqIo-Bsi_qoBqV9JAu7cUXvG0UzZmFrSPk/edit?usp=sharing",
                target="_blank",
            ),
            html.Hr(style={"margin": "30px 0"}),
            html.H2("Methodology"),
            html.P(
                "Search of messages, tweets, and news evoking incidents on Russian Railways Network."
            ),
            html.Img(
                src="/assets/images/architecture_project.png",
                style={"maxWidth": "100%", "height": "auto", "margin": "20px 0"},
            ),
            html.Hr(style={"margin": "30px 0"}),
            # Three columns layout using Dash
            html.Div(
                className="three-columns",
                children=[
                    # Column 1
                    html.Div(
                        className="column",
                        children=[
                            html.H3("Twitter X Accounts"),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.A(
                                                acc,
                                                href=url,
                                                target="_blank",
                                            )
                                        ]
                                    )
                                    for acc, url in dict_twitter.items()
                                ]
                            ),
                        ],
                        style={"flex": "1", "padding": "0 10px"},
                    ),
                    # Column 2
                    html.Div(
                        className="column",
                        children=[
                            html.H3("Telegram Channels"),
                            html.Ul(
                                [
                                    html.Li(
                                        [
                                            html.A(
                                                acc,
                                                href=url,
                                                target="_blank",
                                            )
                                        ]
                                    )
                                    for acc, url in dict_telegram.items()
                                ]
                            ),
                        ],
                        style={"flex": "1", "padding": "0 10px"},
                    ),
                    # Column 3 (empty as in the original)
                    html.Div(
                        className="column",
                        children=[html.P("")],
                        style={"flex": "1", "padding": "0 10px"},
                    ),
                ],
                style={"display": "flex", "flexWrap": "wrap"},
            ),
        ],
    )


layout = html.Div(
    className="page-content",
    children=[
        html.H1(
            className="page-title",
            children="🚂 Incidents Russian Railways Analytics 🇷🇺",
        ),
        dcc.Tabs(
            id="tabs-container",
            className="tabs-container",
            value="tab-overview",
            children=[
                dcc.Tab(
                    label="Overview",
                    value="tab-overview",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Incidents Types",
                    value="tab-incidents-types",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Damaged Equipments",
                    value="tab-damaged-equipments",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Collisions",
                    value="tab-collisions",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Sabotage",
                    value="tab-sabotage",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Partisans Arrest",
                    value="tab-partisans-arrest",
                    className="tab",
                    selected_className="tab--selected",
                ),
                dcc.Tab(
                    label="Informations",
                    value="tab-informations",
                    className="tab",
                    selected_className="tab--selected",
                ),
            ],
        ),
        html.Div(id="tabs-content"),
    ],
)


@callback(
    Output("tabs-content", "children"),
    Input("tabs-container", "value"),
)
def render_content(tab):
    tab_functions = {
        "tab-overview": tab_overview,
        "tab-incidents-types": tab_incidents_types,
        "tab-damaged-equipments": tab_damaged_equipments,
        "tab-collisions": tab_collisions,
        "tab-sabotage": tab_sabotage,
        "tab-partisans-arrest": tab_partisans_arrest,
        "tab-informations": tab_informations,
    }

    if tab in tab_functions:
        return html.Div([tab_functions[tab]()])
    return html.Div([])


@callback(
    Output("rail_1_fig6", "figure"),
    Input("togl_rail_1_fig6", "on"),
)
def update_treemap(value):
    # #########################################
    # #########################################
    # Number of Incidents by Region
    # Treemap

    if value:
        # Remove label "Moscow"
        df = df_incd_reg_total[df_incd_reg_total["label"] != "Moscow"]
    else:
        # Add label "Moscow"
        df = df_incd_reg_total

    fig6 = generate_treemap2(
        title="Distribution of Railway Incidents by Region in Russia",
        subtitle="",
        ids_=df["label"],
        labels_=df["label"],
        parents_=[""] * len(df),
        values_=df["total_inc"],
        colors=df["total_inc"],
    )

    return fig6


@callback(
    Output("rail_1_fig7", "figure"),
    Input("togl_rail_1_fig7", "on"),
)
def update_treemap(value):
    # #########################################
    # #########################################
    # Number of Incidents by Region
    # Map

    if value:
        # Remove label "Moscow"
        df = df_map_inc_reg[df_map_inc_reg["label"] != "Moscow"]
    else:
        # Add label "Moscow"
        df = df_map_inc_reg

    fig7 = generate_map(
        title="Geographic Distribution of Sabotages on Russian Railways",
        subtitle="Number of Railway Sabotages by Region in Russia from 2023 to 2024",
        polygons=polygons,
        loc=df["id_region"],
        text_=df["region"],
        z_=df["total_inc"],
        locationmode="geojson-id",
    )
    fig7 = fig_upd_layout(
        fig7,
        geo=dict(
            projection=dict(type="eckert1", scale=3.4),
            center=dict(lat=62, lon=98),
        ),
        dragmode=False,
    )
    return fig7
