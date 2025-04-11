import pandas as pd
import dash
from dash import Dash, html, dcc
import plotly.express as px
import plotly.graph_objects as go
import plotly.colors as pc

from variables import PATH_DMT_COMPO_WEAPONS, COLORS_COMP_WEAPONS

dash.register_page(__name__)

theme = {
    "dark": True,
    "detail": "#007439",
    "primary": "#00EA64",
    "secondary": "#6E6E6E",
}

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


def fig_upd_layout(fig):
    fig.update_layout(
        font=dict(color="#FFFFFF"),
        paper_bgcolor="#222222",
        plot_bgcolor="#222222",
        yaxis=dict(ticks="outside", ticklen=2),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(255, 255, 255, 0.3)",
        ),
    )

    return fig


#########################################
#########################################
# Components by equipment type
# Pie chart
df = dmt_global[dmt_global["libelle"] == "type_equipment"]
fig1 = go.Figure(
    go.Pie(
        labels=df["metric"],
        values=df["count"],
        textinfo=f"label+percent+value",
        hoverinfo="label+percent+value",
        hovertemplate="%{label}<br>%{value} Components<extra></extra>",
        textposition="auto",
        hole=0.15,
        marker=dict(
            colors=[COLORS_COMP_WEAPONS[x] for x in df["metric"]],
            line=dict(width=0.4, color="#0E1117"),
        ),
    ),
    layout=dict(
        title=dict(
            text="Weapon Components by Equipment Type",
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
fig1 = fig_upd_layout(fig1)


#########################################
#########################################
# Components by weapon type
# Bar chart

df = dmt_global[dmt_global["libelle"] == "weapon_type"]
df = df.sort_values(by="count", ascending=False)
fig2 = go.Figure(
    go.Bar(
        x=df["metric"],
        y=df["count"],
        marker=dict(
            color="#196481",
            cornerradius=4,
        ),
        text=df["count"],
        textposition="outside",
        hoverinfo="y+text",
        hovertemplate="%{x} <br> %{y} Components<extra></extra>",
    ),
    layout=dict(
        title=dict(
            text="Weapon Components by Weapon Type",
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by weapon type",
            ),
            xanchor="center",
            x=0.5,
        ),
        height=500,
        xaxis_title="Weapon Type",
        yaxis_title="Number of Components",
    ),
)
fig2.update_layout(
    font=dict(color="#FFFFFF"),
    paper_bgcolor="#222222",
    plot_bgcolor="#222222",
    yaxis=dict(
        showgrid=True,
        gridcolor="rgba(255, 255, 255, 0.3)",
    ),
)


#########################################
#########################################
# Components by Manufacturer country
# Bar chart

df = dmt_global[dmt_global["libelle"] == "manufacturer_country"]
df = df.sort_values(by="count", ascending=True)
fig3 = go.Figure(
    go.Bar(
        orientation="h",
        x=df["count"],
        y=df["metric"],
        marker=dict(
            color="#817f19",
            cornerradius=4,
        ),
        text=df["count"],
        textposition="outside",
    ),
    layout=dict(
        title=dict(
            text="Weapon Components by Manufacturer Country",
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
fig3 = fig_upd_layout(fig3)

#########################################
#########################################
# Components by Manufacturer
# Bar chart
df = dmt_global[dmt_global["libelle"] == "manufacturer"].head(30)
df = df.sort_values(by="count", ascending=True)
fig4 = go.Figure(
    go.Bar(
        orientation="h",
        x=df["count"],
        y=df["metric"],
        marker=dict(
            color="#19815e",
            cornerradius=4,
        ),
        text=df["count"],
        # textfont=dict(size=12),
    ),
    layout=dict(
        title=dict(
            text="Weapon Components by Manufacturer (Top 30)",
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
fig4 = fig_upd_layout(fig4)


#########################################
#########################################
# Components by Weapon
# Bar chart
df = dmt_global[dmt_global["libelle"] == "weapon"].head(60)
df = df.sort_values(by="count", ascending=True)

df["metric"] = df["metric"].apply(lambda x: x[:50] + "..." if len(x) > 50 else x)

fig5 = go.Figure(
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
            text="Components Found in Weapon (Top 60)",
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
fig5 = fig_upd_layout(fig5)


#########################################
#########################################
# TreeMap by Country and Weapon Type
# TreeMap
df = dmt_treemap_country_weapon_type


# Create a color mapping function
def get_color(label):
    return COLORS_COMP_WEAPONS.get(label, "#3a3939")


# Map colors to each label in the dataframe
df["color"] = df["label"].apply(get_color)

fig6 = go.Figure(
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
            text="Weapon Components by Manufacturer Country and Weapon Type (Top 10 Countries)",
            pad=dict(l=4),
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by manufacturer country and weapon type for the top 10 countries",
            ),
        ),
        height=800,
        font=dict(size=14.5),
    ),
)

fig6.update_traces(marker=dict(cornerradius=4))
fig6 = fig_upd_layout(fig6)


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
fig7 = go.Figure(
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
            text="Weapon Components by Equipment Type and Weapon Type",
            pad=dict(l=4),
            subtitle=dict(
                text=f"Number of components found in the aggressor's weapons by equipment type and weapon type",
            ),
        ),
        height=800,
    ),
)
fig7 = fig_upd_layout(fig7)


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

fig8 = go.Figure()


for country in sorted(df["manufacturer_country"].unique()):
    country_data = df[df["manufacturer_country"] == country]

    temp_df = pd.DataFrame({"weapon": weapons_sorted})
    temp_df = temp_df.merge(country_data, on="weapon", how="left").fillna(0)

    fig8.add_trace(
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

fig8.update_layout(
    barmode="stack",
    title=dict(
        text="Weapon Components by Manufacturer Country and Weapon",
        pad=dict(l=8),
        subtitle=dict(
            text=f"Number of components found in the aggressor's weapons by manufacturer country and weapon",
        ),
        xanchor="center",
        x=0.5,
    ),
    height=1200,
    xaxis_title="Number of Components",
    yaxis_title="Weapons",
)
fig8 = fig_upd_layout(fig8)


layout = html.Div(
    className="page-content",
    children=[
        html.H1(
            className="page-title",
            children="🚂 Incidents Russian Railways Analytics 🇷🇺",
        )
    ],
)
