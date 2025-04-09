import datetime as dt
import pandas as pd

import streamlit as st

import plotly.express as px
import plotly.graph_objects as go
import plotly.colors as pc

from variables import PATH_DMT_COMPO_WEAPONS, COLORS_COMP_WEAPONS
from utils import jump_lines, init_css, add_analytics_tag, developper_link

# from create_charts import (
#     create_bar,
#     create_pie,
# )

# Google Analytics
add_analytics_tag()

# CSS
init_css()


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

#############
## SIDEBAR ##
#############
with st.sidebar:
    developper_link()


###############
## MAIN PAGE ##
###############

st.title("⚙️ Components Weapons")
st.error(
    """
    The data has been collected from **[War & Sanctions - Сomponents in the aggressor`s weapon](https://war-sanctions.gur.gov.ua/en/components)**
    
    This page is a graphical representation of the data.
    """,
    icon="⚠️",
)
jump_lines(1)
st.divider()


# st.write("## Global Count")


col1, col2 = st.columns([0.5, 0.5])


#########################################
#########################################
# Components by equipment type
# Pie chart

df = dmt_global[dmt_global["libelle"] == "type_equipment"]
fig = go.Figure(
    go.Pie(
        labels=df["metric"],
        values=df["count"],
        textinfo=f"label+percent+value",
        hoverinfo="label+percent+value",
        hovertemplate="%{label} <br> %{value} Components<extra></extra>",
        textfont=dict(size=12),
        textposition="auto",
        hole=0.15,
        marker=dict(
            colors=[COLORS_COMP_WEAPONS[x] for x in df["metric"]],
            line=dict(width=0.4, color="#0E1117"),
        ),
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Equipment Type",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by equipment type",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=500,
        showlegend=False,
    ),
)
col1.plotly_chart(fig)


#########################################
#########################################
# Components by weapon type
# Bar chart

df = dmt_global[dmt_global["libelle"] == "weapon_type"]
df = df.sort_values(by="count", ascending=False)
fig = go.Figure(
    go.Bar(
        x=df["metric"],
        y=df["count"],
        marker=dict(
            color="#196481",
            cornerradius=4,
        ),
        text=df["count"],
        textposition="outside",
        textfont=dict(size=12),
        hoverinfo="y+text",
        hovertemplate="%{x} <br> %{y} Components<extra></extra>",
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Weapon Type",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by weapon type",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=550,
        xaxis_title="Weapon Type",
        yaxis_title="Number of Components",
    ),
)
col2.plotly_chart(fig)

jump_lines(3)
col1, col2 = st.columns([0.43, 0.57])


#########################################
#########################################
# Components by Manufacturer country
# Bar chart

df = dmt_global[dmt_global["libelle"] == "manufacturer_country"]
df = df.sort_values(by="count", ascending=True)
fig = go.Figure(
    go.Bar(
        orientation="h",
        x=df["count"],
        y=df["metric"],
        marker=dict(
            color="#817f19",
            cornerradius=4,
        ),
        text=df["count"],
        textfont=dict(size=12),
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Manufacturer Country",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by manufacturer country",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=850,
        xaxis_title="Number of Components",
        yaxis_title="Countries",
    ),
)
col1.plotly_chart(fig)


#########################################
#########################################
# Components by Manufacturer
# Bar chart
df = dmt_global[dmt_global["libelle"] == "manufacturer"].head(30)
df = df.sort_values(by="count", ascending=True)
fig = go.Figure(
    go.Bar(
        orientation="h",
        x=df["count"],
        y=df["metric"],
        marker=dict(
            color="#19815e",
            cornerradius=4,
        ),
        text=df["count"],
        textfont=dict(size=12),
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Manufacturer (Top 30)",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by manufacturer (Top 30)",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=850,
        xaxis_title="Number of Components",
        yaxis_title="Manufacturers",
    ),
)
col2.plotly_chart(fig)

with col2.expander("See all manufacturers"):
    st.dataframe(
        dmt_global[dmt_global["libelle"] == "manufacturer"]
        .sort_values(by="count", ascending=False)
        .drop(columns=["libelle"])
        .rename(columns={"metric": "manufacturer"})
    )


st.divider()

#########################################
#########################################
# Components by Weapon
# Bar chart
df = dmt_global[dmt_global["libelle"] == "weapon"].head(60)
df = df.sort_values(by="count", ascending=True)

df["metric"] = df["metric"].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

fig = go.Figure(
    go.Bar(
        orientation="h",
        x=df["count"],
        y=df["metric"],
        marker=dict(
            color="#a81515",
            cornerradius=4,
        ),
        text=df["count"],
        textposition="outside",
        textfont=dict(size=12),
    ),
    layout=dict(
        title=dict(
            text="Distribution of Components Found in Weapon (Top 60)",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by weapon (Top 60)",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=1200,
        xaxis_title="Number of Components",
        yaxis_title="Weapons",
    ),
)
st.plotly_chart(fig)
with st.expander("See all weapons"):
    st.dataframe(
        dmt_global[dmt_global["libelle"] == "weapon"]
        .sort_values(by="count", ascending=False)
        .drop(columns=["libelle"])
        .rename(columns={"metric": "weapon"}),
        use_container_width=True,
    )

st.divider()


#########################################
#########################################
# TreeMap by Country and Weapon Type
# TreeMap

if st.checkbox("Hide USA from TreeMap"):
    # Hide USA from the TreeMap
    df = dmt_treemap_country_weapon_type[
        dmt_treemap_country_weapon_type["id"].str.contains("USA") == False
    ]
else:
    # Show USA in the TreeMap
    df = dmt_treemap_country_weapon_type


# Create a color mapping function
def get_color(label):
    return COLORS_COMP_WEAPONS.get(label, "#3a3939")


# Map colors to each label in the dataframe
df["color"] = df["label"].apply(get_color)

fig = go.Figure(
    go.Treemap(
        ids=df["id"],
        labels=df["label"],
        parents=df["parent"],
        values=df["value"],
        marker=dict(
            colors=df["color"],
        ),
        textinfo="label+percent parent+value",
        branchvalues="total",
        hovertemplate="%{label} <br> %{value} Components<extra></extra>",
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Manufacturer Country and Weapon Type (Top 10 Countries)",
            pad=dict(l=4),
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by manufacturer country and weapon type for the top 10 countries",
            ),
        ),
        height=800,
        font=dict(size=14.5),
    ),
)

fig.update_traces(marker=dict(cornerradius=4))
st.plotly_chart(fig)


###########################################
###########################################
# Sunburst by Equipment Type and Weapon Type
# Sunburst
df = dmt_sunburst_equipments_weapon_type


# Create a color mapping function
def get_color(label):
    return COLORS_COMP_WEAPONS.get(label, "#3a3939")


# Map colors to each label in the dataframe
df["color"] = df["label"].apply(get_color)
fig = go.Figure(
    go.Sunburst(
        ids=df["id"],
        labels=df["label"],
        parents=df["parent"],
        values=df["value"],
        branchvalues="total",
        textinfo="label+percent parent",
        hovertemplate="%{label} <br> %{value} Components<extra></extra>",
        marker=dict(
            line=dict(width=0.4, color="#0E1117"),
            colors=df["color"],
            colorscale="YlOrRd",
        ),
    ),
    layout=dict(
        title=dict(
            text="Distribution of Weapon Components by Equipment Type and Weapon Type",
            pad=dict(l=4),
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by equipment type and weapon type",
            ),
        ),
        height=800,
    ),
)
st.plotly_chart(fig)
st.divider()


###########################################
###########################################
# Components by Weapon
# Stacked Bar chart
df = dmt_country_by_weapon

# Get only 50 first caracters
df["weapon"] = df["weapon"].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

# Create a color mapping for the countries
colorscale = pc.sequential.Plasma + pc.sequential.OrRd + pc.sequential.Viridis_r
country_colors = {
    country: colorscale[i % len(colorscale)]
    for i, country in enumerate(sorted(df["manufacturer_country"].unique()))
}

# Sort the weapons by count
weapons_sorted = (
    df.groupby("weapon")["count"].sum().sort_values(ascending=True).index.tolist()
)

fig = go.Figure()


for country in sorted(df["manufacturer_country"].unique()):
    country_data = df[df["manufacturer_country"] == country]

    temp_df = pd.DataFrame({"weapon": weapons_sorted})
    temp_df = temp_df.merge(country_data, on="weapon", how="left").fillna(0)

    fig.add_trace(
        go.Bar(
            name=country,
            y=temp_df["weapon"],
            x=temp_df["count"],
            orientation="h",
            marker_color=country_colors.get(country, "#333"),
            text=temp_df["count"],
            hovertemplate="%{y}<br>%{x} Components<br>" + country,
            hoverinfo="text",
        )
    )

fig.update_layout(
    barmode="stack",
    title=dict(
        text="Distribution of Weapon Components by Manufacturer Country and Weapon",
        pad=dict(l=8),
        subtitle=dict(
            text=f"Number of components found in the aggressor's weapons by manufacturer country and weapon",
        ),
        xanchor="center",
        x=0.5,
    ),
    height=1500,
    xaxis_title="Number of Components",
    yaxis_title="Weapons",
)
st.plotly_chart(fig, use_container_width=True)
st.divider()
