import streamlit as st
import streamlit.components.v1 as components
import pandas as pd

import streamlit_shadcn_ui as ui
import plotly.graph_objects as go
from variables import colors, datamarts_path


# Charts
from create_charts import (
    create_pie,
    create_bar,
    create_treemap,
    create_sunburst,
    create_line,
    create_sankey,
    create_heatmap,
    create_map,
    create_wordcloud,
    create_waffle,
    create_waterfall,
)


st.set_page_config(page_title="Partisans Data Analysis", page_icon="📊", layout="wide")
st.title("🚂 Incidents Russian Railways Analytics 🇷🇺")


with st.sidebar:
    st.title("Menu")

    with st.container():
        components.html(
            """
            <div style="display: flex; align-items: center; justify-content: center;">
                <a href="https://twitter.com/kyytox" target="_blank">
                    <img src="https://pbs.twimg.com/profile_images/1471129038022455299/Zn05GePO_400x400.jpg" style="width: 30px; border-radius: 50%;">
                </a>
                <a href="https://twitter.com/kyytox" target="_blank" style="margin-left: 10px; font-size: 0.9rem; color: #ffffff; text-decoration: none; width: 100%;">
                    Developed by: Kyytox
                </a>
            </div>
            """,
            height=50,
            width=200,
        )

# update CSS
st.markdown(
    """<style>
    
    .st-emotion-cache-1jicfl2 {
        padding-left: 3rem;
        padding-right: 3rem;
        padding-bottom: 3rem;
        padding-top: 3rem;
    }     
    .stTabs [data-baseweb="tab-list"] {
		gap: 2px;
    }

	.stTabs [data-baseweb="tab"] {
		height: 35px;
        white-space: pre-wrap;
		background-color: #979797;
		border-radius: 4px 4px 0px 0px;
        padding: 0px 10px;
        color: #000000;
    }
    
	.stTabs [data-baseweb="tab"]:nth-child(6) {
		background-color: #919191;
    }

	.stTabs [aria-selected="true"] {
        background-color: #FFFFFF;
        
    }
    
    footer:after {
        content:'goodbye'; 
        visibility: visible;
        display: block;
        position: relative;
        #background-color: red;
        padding: 5px;
        top: 2px;
    }


    /* sidebar box (develop by + avatar) */
    .stSidebar [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {
        position: fixed;
        bottom: 0;
    }
    .stSidebar [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="element-container"]:first-child {
        width: 40px;
    }
    
    /* use this code inside your css file */
    div.stMetric{
        background-color: #1a212e;
        border: 2px solid;
        padding: 20px 20px 20px 20px;
        border-radius: 10px;
        color: #929292;
        box-shadow: 10px;
        height: 8rem;
    }
    div.stMetric p{
        color: #ffffff;
        font-size: 1rem;
    }
    div.stMetric [data-testid="stMetricValue"]{
        color: #ffffff;
        font-weight: 700;
    }

            
    </style>""",
    unsafe_allow_html=True,
)


# -----------DATAMARTS-----------
dmt_inc_total = pd.read_parquet(f"{datamarts_path}/inc_total.parquet")
dmt_inc_year = pd.read_parquet(f"{datamarts_path}/inc_by_year.parquet")
dmt_inc_month = pd.read_parquet(f"{datamarts_path}/inc_by_month.parquet")
dmt_inc_cumul_month = pd.read_parquet(f"{datamarts_path}/inc_cumul_by_month.parquet")
dmt_inc_region = pd.read_parquet(f"{datamarts_path}/inc_by_region.parquet")
dmt_inc_day_week = pd.read_parquet(f"{datamarts_path}/inc_by_day_week.parquet")
dmt_inc_dmg_eqp = pd.read_parquet(f"{datamarts_path}/inc_type_dmg_eqp.parquet")
dmt_sun_tree = pd.read_parquet(f"{datamarts_path}/inc_type_dmg_eqp_sun_tree.parquet")
dmt_app_laws_prtsn_age = pd.read_parquet(f"{datamarts_path}/app_laws_prtsn_age.parquet")
dmt_wordcloud = pd.read_parquet(f"{datamarts_path}/wordcloud.parquet")
dmt_sab_prtsn_grp = pd.read_parquet(f"{datamarts_path}/sabotage_by_prtsn_grp.parquet")

# st.write("total")
# st.dataframe(dmt_inc_total)
# st.write("year")
# st.dataframe(dmt_inc_year)
# st.write("month")
# st.dataframe(dmt_inc_month)
# st.write("cumul_month")
# st.dataframe(dmt_inc_cumul_month)
# st.write("day_week")
# st.dataframe(dmt_inc_day_week)
# st.write("region")
# st.dataframe(dmt_inc_region)
# st.write("incident_dmg_eqp")
# st.dataframe(dmt_inc_dmg_eqp)
# st.write("sun_tree")
# st.dataframe(dmt_sun_tree)

# -----------DATAMARTS-----------


# -----------LISTS-----------
lst_inc_type = dmt_inc_total[dmt_inc_total["type"] == "inc_type"]["label"].unique()
lst_dmg_eqp = dmt_inc_total[dmt_inc_total["type"] == "dmg_eqp"]["label"].unique()
lst_sabotage_dmg_eqp = dmt_inc_total[dmt_inc_total["type"] == "sabotage"][
    "label"
].unique()
list_partisans_group = dmt_inc_total[dmt_inc_total["type"] == "prtsn_grp"][
    "label"
].unique()

# st.write("lst_inc_type")
# st.write(lst_inc_type)
# st.write("lst_dmg_eqp")
# st.write(lst_dmg_eqp)
# st.write("lst_sabotage_dmg_eqp")
# st.write(lst_sabotage_dmg_eqp)

# -----------LISTS-----------

# -----------DF CHARTS-----------

df_incd_typ_total = dmt_inc_total[dmt_inc_total["type"] == "inc_type"]
df_dmg_eqp_total = dmt_inc_total[dmt_inc_total["type"] == "dmg_eqp"]
df_incd_reg_total = dmt_inc_total[dmt_inc_total["type"] == "region"]
df_inc_type_cumul_month = dmt_inc_cumul_month[dmt_inc_cumul_month["inc_type"].notnull()]
df_dmg_eqp_cumul_month = dmt_inc_cumul_month[dmt_inc_cumul_month["dmg_eqp"].notnull()]

# -----------DF CHARTS-----------


# -----------VARIABLES-----------

sufix_subtitle = "on the Russian Railways Network in 2023 and 2024"

# -----------VARIABLES-----------


# Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    [
        "Overview",
        "Incidents Types",
        "Damaged Equipments",
        "Sabotage",
        "Partisans Arrest",
        "Informations",
    ]
)


with tab1:
    st.title("Overview")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Total Incidents",
        int(dmt_inc_total[dmt_inc_total["label"] == "Total"]["total_inc"].values[0]),
    )
    col2.metric(
        "Total Incidents 2024",
        int(dmt_inc_year["2024"][dmt_inc_year["label"] == "Total"].values[0]),
        f"{int(dmt_inc_year['2024'][dmt_inc_year['label'] == 'Total'].values[0] - dmt_inc_year['2023'][dmt_inc_year['label'] == 'Total'].values[0])} from 2023",
    )
    col3.metric("Region Most Impacted", "Moscow")
    col4.metric(
        "Arrested Partisans",
        int(dmt_inc_total[dmt_inc_total["label"] == "Arrested"]["total_inc"].values[0]),
    )

    st.divider()
    col1, col2 = st.columns([0.9, 1.1])

    # -------------------------------
    # Number of Incidents by Year
    # Bar Chart
    with col1:
        df = (
            dmt_inc_year[dmt_inc_year["label"] == "Total"]
            .drop(columns="type")
            .melt(id_vars=["label"], var_name="year", value_name="total_inc")
        )
        fig = create_bar(
            multi=False,
            df=df,
            col_x="year",
            col_y="total_inc",
            title="Annual Railway Incidents in Russia",
            subtitle=f"Number of Incidents {sufix_subtitle}",
        )

        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Wordcloud
    with col2:
        for i in range(5):
            st.write("")
        fig = create_wordcloud(dmt_wordcloud)
        st.pyplot(fig)

    st.divider()
    col1, col2 = st.columns([1, 1])

    # -------------------------------
    # Number of Incidents by Month
    # Line Chart
    with col1:
        df = (
            dmt_inc_month[
                (dmt_inc_month["inc_type"].isnull() & dmt_inc_month["dmg_eqp"].isnull())
            ]
            .pivot(index="month", columns="year", values="total_inc")
            .reset_index()
        )

        fig = create_line(
            df,
            col_x="month",
            title="Monthly Incident Trends on Russian Railways",
            subtitle=f"Number of Incidents by Month {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Incidents by Month
    # Bar Chart
    with col2:
        df = dmt_inc_month[
            (dmt_inc_month["inc_type"].isnull() & dmt_inc_month["dmg_eqp"].isnull())
        ]
        fig = create_bar(
            multi=False,
            df=df,
            col_x="month_year",
            col_y="total_inc",
            title="Monthly Incident Trends on Russian Railways",
            subtitle=f"Number of Incidents by Month {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([1, 1])

    # -------------------------------
    # Number of Incidents Types
    # Bar Chart
    with col1:
        fig = create_bar(
            multi=False,
            df=df_incd_typ_total,
            col_x="label",
            col_y="total_inc",
            title="Distribution of Railway Incidents by Type in Russia",
            subtitle=f"Total number of reported incidents categorized by type {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Damaged Equipments
    # Bar Chart
    with col2:
        fig = create_bar(
            multi=False,
            df=df_dmg_eqp_total,
            col_x="label",
            col_y="total_inc",
            title="Distribution of Railway Damaged Equipments in Russia",
            subtitle=f"Total number of reported Damaged Equipments {sufix_subtitle}",
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.warning(
        "Warning: Crimea is represented on this map, it is good to remember that Crimea is Ukrainian, but being occupied the rail network is under the responsibility of the Orcs."
    )
    # -------------------------------
    # Number of Incidents by Region
    # Treemap
    fig = create_treemap(
        ids=df_incd_reg_total["label"],
        labels=df_incd_reg_total["label"],
        parents=[""] * len(df_incd_reg_total),
        values=df_incd_reg_total["total_inc"],
        title="Distribution of Railway Incidents by Region in Russia",
    )
    st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Incidents by Region
    # Map

    fig = create_map(
        df_incd_reg_total,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # -------------------------------
    # Number of Incidents by Day
    # Heatmap
    list_years = dmt_inc_day_week.columns.str.split("_").str[0].unique()
    for year in list_years:
        fig = create_heatmap(dmt_inc_day_week, year)
        st.plotly_chart(fig, use_container_width=True)


with tab2:
    st.title("Incidents Types")

    col1, col2 = st.columns([0.8, 1.2])

    # -------------------------------
    # Number of Incidents by Incident Type
    # Pie Chart
    with col1:
        fig = create_pie(
            df_incd_typ_total["label"],
            df_incd_typ_total["total_inc"],
            title="Distribution of Incidents by Type",
            subtitle=f"Distribution of reported incidents categorized by type {sufix_subtitle}",
            center_txt="Incidents",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Evolution of Incidents by Incident Type
    # Line Chart
    with col2:
        df = df_inc_type_cumul_month.pivot(
            index="month_year", columns="inc_type", values="cumul_inc"
        ).reset_index()
        fig = create_line(
            df,
            col_x="month_year",
            title="Trends in Cumulative Incidents by Type Over Time",
            subtitle=f"Cumulative number of incidents by type {sufix_subtitle}",
            mode="lines",
            fill="none",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([0.72, 1.28])

    # -------------------------------
    # Number of Incidents by Incident Type by Year
    # Bar Chart
    with col1:
        df = dmt_inc_year[(dmt_inc_year["type"] == "inc_type")]
        fig = create_bar(
            multi=True,
            df=df,
            col_x="label",
            col_y=None,
            title="Incidents by Type: Yearly Comparison for 2023 and 2024",
            subtitle=f"Insight into the distribution of incident types {sufix_subtitle}",
            start_col=2,
            bar_mode="group",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Incidents by Incident Type by Month
    # Bar Chart
    with col2:
        df = (
            dmt_inc_month[
                (
                    dmt_inc_month["inc_type"].notnull()
                    & dmt_inc_month["dmg_eqp"].isnull()
                )
            ]
            .pivot(index="month_year", columns="inc_type", values="total_inc")
            .reset_index()
        )
        fig = create_bar(
            multi=True,
            df=df,
            col_x="month_year",
            col_y=None,
            title="Monthly Incidents by Type for 2023 and 2024",
            subtitle=f"A detailed view of incidents categorized by type on a monthly {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([0.8, 1.2])

    # ---------------------------------
    #  Sunburst chart
    with col1:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "inc_type") & (dmt_sun_tree["chart"] == "sun_tree")
        ]
        fig = create_sunburst(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            title="Distribution of Damaged Equipment by Incident Type",
            subtitle=f"Relationship between incident types and damaged equipment {sufix_subtitle}",
            map_colors=[colors[label] for label in df["label"]],
        )
        st.plotly_chart(fig, use_container_width=True)

    # ---------------------------------
    # Sankey diagram
    with col2:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "inc_type") & (dmt_sun_tree["chart"] == "sankey")
        ].reset_index(drop=True)
        fig = create_sankey(
            df=df,
            colx="inc_type",
            coly="dmg_eqp",
            title="Flow of Damaged Equipment by Incident Type",
            subtitle=f"Visual representation of how incident types lead to equipment damage {sufix_subtitle}",
            df_total=dmt_inc_dmg_eqp,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # -------------------------------
    # Number of Incidents by Incident Type by Region
    # Bar Chart
    df = dmt_inc_region[["region"] + list(lst_inc_type)]
    df["region"] = pd.Categorical(
        df["region"], categories=df_incd_reg_total["label"], ordered=True
    )
    df = df.sort_values("region", ascending=False)

    fig = create_bar(
        multi=True,
        df=df,
        col_x="region",
        col_y=None,
        title="Distribution of Incidents by Type and Region",
        orient="h",
    )
    st.plotly_chart(fig, use_container_width=True)


with tab3:
    st.title("Damaged Equipments")

    col1, col2 = st.columns([0.8, 1.2])

    # -------------------------------
    # Number of Incidents by Damaged Equipment
    # Pie Chart
    with col1:
        fig = create_pie(
            df_dmg_eqp_total["label"],
            df_dmg_eqp_total["total_inc"],
            title="Distribution of Damaged Equipments",
            subtitle=f"Distribution of damaged equipments {sufix_subtitle}",
            center_txt="Damaged Equipment",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Evolution of Incidents by Incident Type
    # Line Chart
    with col2:
        df = df_dmg_eqp_cumul_month.pivot(
            index="month_year", columns="dmg_eqp", values="cumul_inc"
        ).reset_index()
        fig = create_line(
            df,
            col_x="month_year",
            title="Trends in Cumulative Damaged Equipments Over Time",
            subtitle=f"Cumulative number of damaged equipments {sufix_subtitle}",
            mode="lines",
            fill="none",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([0.8, 1.2])

    # -------------------------------
    # Number of Incidents by Damaged Equipment by Year
    # Bar Chart
    with col1:
        df = dmt_inc_year[(dmt_inc_year["type"] == "dmg_eqp")]
        fig = create_bar(
            multi=True,
            df=df,
            col_x="label",
            col_y=None,
            title="Damaged Equipment: Yearly Comparison for 2023 and 2024",
            subtitle=f"Insight into the distribution of damaged equipment {sufix_subtitle}",
            start_col=2,
            bar_mode="group",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Incidents by Damaged Equipment by Month
    # Bar Chart
    with col2:
        df = (
            dmt_inc_month[
                (
                    dmt_inc_month["inc_type"].isnull()
                    & dmt_inc_month["dmg_eqp"].notnull()
                )
            ]
            .pivot(
                index="month_year",
                columns="dmg_eqp",
                values="total_inc",
            )
            .reset_index()
        )
        fig = create_bar(
            multi=True,
            df=df,
            col_x="month_year",
            col_y=None,
            title="Monthly Damaged Equipments for 2023 and 2024",
            subtitle=f"A detailed view of Damaged Equipments on a monthly {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([0.8, 1.2])

    # ---------------------------------
    #  Sunburst chart
    with col1:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "dmg_eqp") & (dmt_sun_tree["chart"] == "sun_tree")
        ]

        fig = create_sunburst(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            title="Distribution of Incident Types by Damaged Equipment",
            subtitle=f"Relationship between damaged equipment and incident types {sufix_subtitle}",
            map_colors=[colors[label] for label in df["label"]],
        )
        st.plotly_chart(fig, use_container_width=True)

    # ---------------------------------
    # Sankey diagram
    with col2:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "dmg_eqp") & (dmt_sun_tree["chart"] == "sankey")
        ].reset_index(drop=True)

        fig = create_sankey(
            df=df,
            colx="dmg_eqp",
            coly="inc_type",
            title="Flow of Incident Types by Damaged Equipment",
            subtitle=f"Visual representation of how damaged equipment leads to incident types {sufix_subtitle}",
            df_total=dmt_inc_dmg_eqp,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # -------------------------------
    # Number of Incidents by Damaged Equipment by Region
    # Bar Chart
    df = dmt_inc_region[["region"] + list(lst_dmg_eqp)]
    df["region"] = pd.Categorical(
        df["region"], categories=df_incd_reg_total["label"], ordered=True
    )
    df = df.sort_values("region", ascending=False)
    fig = create_bar(
        multi=True,
        df=df,
        col_x="region",
        col_y=None,
        title="Distribution of Damaged Equipment by Region",
        orient="h",
    )
    st.plotly_chart(fig, use_container_width=True)


with tab4:
    st.title("Sabotage")

    col1, col2 = st.columns([0.7, 1.3])

    # -------------------------------
    # Number of Sabotage by Year
    # Bar Chart
    with col1:
        df = (
            dmt_inc_year[(dmt_inc_year["label"] == "Sabotage")]
            .drop(columns="type")
            .melt(id_vars=["label"], var_name="year", value_name="total_inc")
        )
        fig = create_bar(
            multi=False,
            df=df,
            col_x="year",
            col_y="total_inc",
            title="Annual Number of Sabotage",
            subtitle=f"Number of Sabotage {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Sabotage by Month
    # Line Chart
    with col2:
        df = (
            dmt_inc_month[
                (dmt_inc_month["inc_type"] == "Sabotage")
                & (dmt_inc_month["dmg_eqp"].isnull())
            ]
            .pivot(index="month", columns="year", values="total_inc")
            .reset_index()
        )
        fig = create_line(
            df,
            col_x="month",
            title="Monthly Number of Sabotage",
            subtitle=f"Number of Sabotage {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # -------------------------------
    # Number of Sabotage by Year, compared to the total number of incidents
    # Line Chart
    df = (
        dmt_inc_month[
            (
                (dmt_inc_month["inc_type"] == "Sabotage")
                & (dmt_inc_month["dmg_eqp"].isnull())
            )
            | (
                (dmt_inc_month["inc_type"].isnull())
                & (dmt_inc_month["dmg_eqp"].isnull())
            )
        ]
        .fillna("Total")
        .pivot(index="month_year", columns="inc_type", values="total_inc")
        .reset_index()
    )
    df = df[["month_year", "Total", "Sabotage"]]
    fig = create_line(
        df,
        col_x="month_year",
        title="Incidents vs Sabotage over Time",
        subtitle=f"Evolution of the number of incidents and sabotage {sufix_subtitle}",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([0.9, 1.1])
    # -------------------------------
    # Distribution of Sabotage by Partisans Group
    # Pie Chart
    with col1:
        df = dmt_inc_total[dmt_inc_total["type"] == "prtsn_grp"]
        fig = create_pie(
            df["label"],
            df["total_inc"],
            title="Distribution of Sabotage by Partisans Group",
            subtitle=f"Distribution of sabotage incidents by partisan groups {sufix_subtitle}",
            center_txt="Partisans Group",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Incident by day over time by partisans group
    # Scatter Chart
    with col2:
        fig = go.Figure(
            go.Scatter(
                x=dmt_sab_prtsn_grp["date"],
                y=dmt_sab_prtsn_grp["prtsn_grp"],
                mode="markers",
                marker=dict(
                    color=[colors[label] for label in dmt_sab_prtsn_grp["prtsn_grp"]]
                ),
            ),
            layout=dict(
                title=dict(
                    text="Sabotage Incidents by Partisans Group",
                    pad=dict(l=20),
                    y=0.99,
                    subtitle=dict(
                        text=f"Sabotage incidents by partisan groups {sufix_subtitle}"
                    ),
                ),
                xaxis=dict(title="Date"),
                height=600,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([1.2, 0.8])

    # -------------------------------
    # Number of Partisans Group by Damaged Equipment
    # Treemap
    with col1:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "dmg_eqp_prtsn_grp")
            & (dmt_sun_tree["chart"] == "sun_tree")
        ]
        fig = create_treemap(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            title="Treemap of Damaged Equipment with Partisan Group Attribution",
            subtitle=f"A hierarchical representation of damaged equipment categorized by the responsible partisan groups {sufix_subtitle}",
            map_colors=[colors[label] for label in df["label"]],
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Sabotage Damaged Equipment by Partisans Group
    # Bar Chart
    with col2:
        df = (
            dmt_inc_total[dmt_inc_total["type"].isin(list_partisans_group)]
            .pivot(index="label", columns="type", values="total_inc")
            .reset_index()
        )
        fig = create_bar(
            multi=True,
            df=df,
            col_x="label",
            col_y=None,
            title="Implication of Partisans Group in Damaged Equipment",
            subtitle=f"Number of damaged equipment by partisan groups {sufix_subtitle}",
            start_col=1,
            legend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns([1.20, 0.8])
    # -------------------------------
    # Number of Damaged Equipment by Partisans Group
    # Treemap
    with col1:
        df = dmt_sun_tree[
            (dmt_sun_tree["tab"] == "prtsn_grp_dmg_eqp")
            & (dmt_sun_tree["chart"] == "sun_tree")
        ]
        fig = create_treemap(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            title="Treemap of Partisan Groups and Associated Equipment Damage",
            subtitle=f"Visualizing the relationship between partisan groups and the equipment they have damaged {sufix_subtitle}",
            map_colors=[colors[label] for label in df["label"]],
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Sabotage Damaged Equipment by Partisans Group
    # Bar Chart
    with col2:
        df = (
            dmt_inc_total[dmt_inc_total["type"].isin(list_partisans_group)]
            .pivot(index="type", columns="label", values="total_inc")
            .reset_index()
        )
        fig = create_bar(
            multi=True,
            df=df,
            col_x="type",
            col_y=None,
            title="Distribution of damaged equipment of partisan groups",
            subtitle=f"Number of damaged equipment by partisan groups {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    #
    st.divider()
    col1, col2 = st.columns([0.8, 1.2])
    # -------------------------------
    # Number of Sabotage by Damaged Equipment
    # Pie Chart
    with col1:
        df = dmt_inc_total[dmt_inc_total["type"] == "sabotage"]
        fig = create_pie(
            df["label"],
            df["total_inc"],
            title="Distribution of Damaged Equipment for Sabotage Incidents",
            subtitle=f"Distribution of damaged equipment for sabotage incidents {sufix_subtitle}",
            center_txt="Act of Sabotage",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Sabotage by Damaged Equipment by Month
    # Bar Chart
    with col2:
        df = (
            dmt_inc_month[
                (dmt_inc_month["inc_type"] == "Sabotage")
                & (dmt_inc_month["dmg_eqp"].notnull())
            ]
            .pivot(
                index="month_year",
                columns="dmg_eqp",
                values="total_inc",
            )
            .reset_index()
        )
        fig = create_bar(
            multi=True,
            df=df,
            col_x="month_year",
            col_y=None,
            title="Monthly Number of Damaged Equipment",
            subtitle=f"A detailed view of damaged equipment for sabotage incidents on a monthly {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    # -------------------------------
    # Number of Sabotage by Damaged Equipment by Region
    # Bar Chart
    df = dmt_inc_region[["region"] + list(lst_sabotage_dmg_eqp)]
    df["region"] = pd.Categorical(
        df["region"], categories=df_incd_reg_total["label"], ordered=True
    )
    df = df.sort_values("region", ascending=False)
    fig = create_bar(
        multi=True,
        df=df,
        col_x="region",
        col_y=None,
        title="Number of Damaged Equipment by Region",
        orient="h",
    )
    st.plotly_chart(fig, use_container_width=True)


with tab5:
    st.title("Partisans Arrest")

    col1, col2 = st.columns([1, 1])

    # -------------------------------
    # Number of Partisans Arrested
    # Pie Chart
    with col1:
        df = dmt_inc_total[dmt_inc_total["type"] == "prtsn_arr"]
        fig = create_pie(
            df["label"],
            df["total_inc"],
            title="Partisans Arrested by Sabotage Incidents",
            subtitle=f"Distribution of partisans arrested for sabotage incidents {sufix_subtitle}",
            center_txt="Act of Sabotage",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Partisans Reward
    # Pie Chart
    with col2:
        df = dmt_inc_total[dmt_inc_total["type"] == "prtsn_rwd"]
        fig = create_pie(
            df["label"],
            df["total_inc"],
            title="Partisans Reward by Partisans Arrested",
            subtitle=f"Distribution of partisans rewarded in partisans arrested for sabotage incidents {sufix_subtitle}",
            center_txt="Partisans Arrested",
        )
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns([0.8, 1.2])

    st.divider()
    # -------------------------------
    # Number of Applicable Laws
    # Treemap
    with col1:
        df = dmt_inc_total[dmt_inc_total["type"] == "app_laws"]
        fig = create_treemap(
            ids=df["label"],
            labels=df["label"],
            parents=[""] * len(df),
            values=df["total_inc"],
            title="Distribution of Applicable Laws",
            subtitle=f"Laws of the Criminal Code of the Russian Federation applied to the partisans arrested <br>for sabotage incidents {sufix_subtitle}",
            map_colors=[colors[label] for label in df["label"]],
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Applicable Laws by Partisans Reward by Partisans Arrested
    # Sankey
    with col2:
        df = dmt_sun_tree[dmt_sun_tree["tab"] == "prtsn_rwd"].reset_index(drop=True)
        fig = create_sankey(
            df=df,
            title="Applicable Laws by Partisans Reward Flow",
            subtitle=f"Visual representation of how applicable laws are related to the reward of partisans arrested for sabotage incidents {sufix_subtitle}",
            colx="label",
            coly="label",
            df_total=dmt_inc_total,
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Partisans Age
    # Waffle Chart
    df = dmt_inc_total[
        (dmt_inc_total["type"] == "prtsn_age")
        & (dmt_inc_total["label"] != "Mean")
        & (~dmt_inc_total["label"].str.startswith("age"))
    ]

    fig = create_waffle(
        df,
    )
    st.pyplot(fig)

    col1, col2 = st.columns([1, 1])

    # -------------------------------
    # Number of Partisans Age
    # Waterfall Chart
    with col1:
        df = dmt_inc_total[
            (dmt_inc_total["type"] == "prtsn_age")
            & (dmt_inc_total["label"].str.startswith("age"))
        ]

        df["label"] = df["label"].str.split("_").str[1]
        df = df.sort_values("label")

        fig = create_waterfall(
            df,
            title="Number of Partisans Arrested by Age",
            subtitle=f"Number of partisans arrested by age for sabotage incidents {sufix_subtitle}",
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Age Group
    # Waterfall Chart
    with col2:
        fig = create_waterfall(
            df,
            title="Number of Partisans Arrested by Age Group",
            subtitle=f"Number of partisans arrested by age group for sabotage incidents {sufix_subtitle}",
            is_group=True,
        )
        st.plotly_chart(fig, use_container_width=True)

    # -------------------------------
    # Number of Applicable Laws by Partisans Age
    # Heatmap
    with col1:
        fig = go.Figure(
            go.Heatmap(
                z=dmt_app_laws_prtsn_age,
                x=dmt_app_laws_prtsn_age.columns,
                y=dmt_app_laws_prtsn_age.index,
                colorscale="Viridis",
                text=dmt_app_laws_prtsn_age.values,
                hovertemplate="age: %{y}<br>law: %{x}<br>application: %{z}",
            ),
            layout=dict(
                title=dict(
                    text="Applicable Laws by Partisans Age",
                    pad=dict(l=20),
                    y=0.99,
                    subtitle=dict(
                        text="Criminal Code Laws Applied Across Different Partisan Age",
                    ),
                ),
                yaxis=dict(title="Partisans Age", type="category"),
                xaxis=dict(tickangle=15),
                height=650,
            ),
        )

        st.plotly_chart(fig, use_container_width=True)


with tab6:
    st.title("Informations")

    st.markdown(
        """
        # Methodology

        ## Data Source

        Search of messages, tweets, and news evoking incidents on Russian Railways Network.

        ### Twitter X

        1. Search Tweets (with keywords)
        2. Filter Tweets (Does it evoke an incident on Russian Railways)
        3. Extract Data
        4. Classify Data

        Search on the following accounts:

        - [Igor Sushko](https://x.com/igorsushko)
        - [Prune60](https://x.com/Prune602)
        - [Intelschizo](https://x.com/Schizointel)
        - [LX](https://x.com/LXSummer1)
        - [NOELreports](https://x.com/NOELreports)

        &nbsp;
        ### Telegram

        1. Extract history of messages
        2. Translate messages (Russian to English)
        3. Filter messages (Does it evoke an incident on Russian Railways)
        4. Extract Data
        5. Classify Data

        Search on the following Channels:

        - [Astra News](https://t.me/astrapress)
        - [SHOT](https://t.me/shot_shot/)
        
        """
    )


# HTML footer
components.html(
    """
    <footer style="color: #ffffff; align-items: center; display: flex; justify-content: flex-end; padding-right: 10px; padding-top: 50px;
    ">
        <a href="https://x.com/Kytox_" target="_blank" style="display: flex; align-items: center; color: #ffffff; text-decoration: none; font-size: 14px;">
        
        <img src="https://pbs.twimg.com/profile_images/1471129038022455299/Zn05GePO_400x400.jpg"
            alt="Kytox"
            style="border-radius: 50%; margin-right: 10px; width: 30px;"
        />
        <p>Developed by: Kyytox</p>
        </a>
        
    </footer>
    """,
    height=100,
)
