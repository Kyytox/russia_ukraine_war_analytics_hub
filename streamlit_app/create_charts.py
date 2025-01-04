import json
import numpy as np
import pandas as pd
import streamlit as st

import matplotlib.pyplot as plt
from pywaffle import Waffle

from wordcloud import WordCloud
import matplotlib.pyplot as plt

import plotly.express as px
import plotly.graph_objects as go

from variables import colors
from utils import get_region


HEIGHT = 650
MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def create_pie(labels, values, title, subtitle="", center_txt="", rotate=320):
    """
    Create a pie chart
    """
    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            marker_colors=[
                colors[elem] if elem in colors else "grey" for elem in labels
            ],
            hole=0.45,
            showlegend=False,
            title=dict(
                text=f"{str(values.sum().astype(int))}<br>{center_txt}",
                font_size=18,
            ),
            textinfo="label+percent+value",
            textfont=dict(size=12),
            rotation=rotate,
            textposition="auto",
        ),
        layout=dict(
            title=dict(
                text=title,
                pad=dict(l=20),
                y=0.99,
                subtitle=dict(
                    text=subtitle,
                ),
            ),
            height=HEIGHT - 66,
            margin=dict(l=0, r=0),
        ),
    )

    return fig


def create_bar(
    multi,
    df,
    col_x,
    col_y,
    title,
    subtitle="",
    start_col=1,
    legend=True,
    bar_mode="stack",
    orient="v",
):
    """
    Create a bar chart

    Args:
        multi: boolean
        df: dataframe
        col_x: str
        col_y: str
        title: str
        start_col: int
        legend: boolean
        bar_mode: str
        orient: str

    Returns:
        fig: plotly.graph_objects.Figure

    """
    # remove NaN values
    df = df.dropna(subset=[col_x])

    if multi:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=df[col_x] if col_x != "region" else df[incident],
                    y=df[incident] if col_x != "region" else df[col_x],
                    name=incident,
                    marker=dict(
                        color=colors[incident],
                        cornerradius=3,
                    ),
                    text=df[incident],
                    showlegend=legend,
                    # hovertemplate="%{x} : %{text}",
                    orientation=orient,
                    textposition="auto",
                )
                for incident in df.columns[start_col:]
            ],
        )
    else:

        def gest_colors(elem):
            if elem in colors:
                return colors[elem]
            else:
                if "2022" in str(elem):
                    return colors["2022"]
                elif "2023" in str(elem):
                    return colors["2023"]
                elif "2024" in str(elem):
                    return colors["2024"]

        fig = go.Figure(
            go.Bar(
                x=df[col_x],
                y=df[col_y],
                text=df[col_y],
                marker=dict(
                    color=(
                        [
                            # colors[elem] if elem in colors else "#774428"
                            gest_colors(elem)
                            for elem in df[col_x]
                        ]
                    ),
                    cornerradius=3,
                ),
                hoverinfo="x+y",
                texttemplate="%{text}",
                name="Incidents",
                textposition="auto",
            ),
        )

    fig.update_layout(
        barmode=bar_mode,
        title=dict(
            text=title,
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text=subtitle,
            ),
        ),
        yaxis_title="Number Incidents",
        height=HEIGHT if col_x != "region" else 1250,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1,
            xanchor="right",
            x=1,
        ),
    )

    if col_x == "year":
        fig.update_xaxes(
            type="category",
        )

    if col_x == "region":
        fig.update_layout(
            yaxis=dict(title="Region"),
            xaxis=dict(title="Number Incidents"),
        )

    if df[col_x].dtype == "datetime64[ns]":
        fig.update_xaxes(
            tickangle=35,
            tickformat="%b %Y",
            tickvals=df[col_x],
            ticktext=[elem.strftime("%b %Y") for elem in df[col_x]],
        )

    return fig


def create_line(
    df, col_x, title, subtitle="", mode="lines+markers+text", fill="tozeroy"
):
    """
    Create a line chart
    """
    # sort Months
    if "month" == col_x:
        df[col_x] = pd.Categorical(df[col_x], categories=MONTHS, ordered=True)
        df = df.sort_values(col_x)

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df[col_x],
                y=df[incident],
                mode=mode,
                fill=fill,
                marker=dict(color=colors[incident]),
                text=df[incident],
                textposition="top left",
                name=incident,
                hovertemplate="Incidents: %{y}<br>Month: %{x}",
            )
            for incident in df.columns[1:]
        ],
        layout=dict(
            title=dict(
                text=title,
                pad=dict(l=20),
                y=0.99,
                subtitle=dict(
                    text=subtitle,
                ),
            ),
            yaxis_title="Number Incidents",
            height=HEIGHT,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1,
                xanchor="right",
                x=1,
            ),
        ),
    )

    fig.update_yaxes()

    # my charts is truncated in extreme values
    if df[col_x].dtype == "int32":
        fig.update_xaxes(range=[df[col_x].min() - 0.12, df[col_x].max() + 0.12])
    elif df[col_x].dtype == "datetime64[ns]":
        fig.update_xaxes(
            tickangle=25,
            tickformat="%b %Y",
            tickvals=df[col_x],
            tickfont=dict(size=10),
        )
    # fig.update_yaxes(ticklabelposition="inside")

    return fig


def create_treemap(ids, labels, parents, values, title, subtitle="", map_colors=None):
    """
    Create a treemap chart
    """
    fig = go.Figure(
        go.Treemap(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(
                colors=map_colors,
                colorscale=None if isinstance(map_colors, list) else "thermal",
            ),
            textinfo="label+percent parent+value",
            branchvalues="total",
            hovertemplate="Incident: %{label}<br>Number Incidents: %{value}",
        ),
        layout=dict(
            title=dict(
                text=title,
                pad=dict(l=20),
                y=0.99,
                subtitle=dict(
                    text=subtitle,
                ),
            ),
            height=HEIGHT,
            font=dict(size=14.5),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1,
                xanchor="right",
                x=0,
            ),
        ),
    )
    fig.update_traces(marker=dict(cornerradius=4))

    return fig


def create_sunburst(ids, labels, parents, values, title, subtitle="", map_colors=None):
    """
    Create a sunburst chart
    """
    fig = go.Figure(
        go.Sunburst(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            marker=dict(
                colors=map_colors,
            ),
            hovertemplate="Incident: %{label}<br>Number Incidents: %{value}",
            textinfo="label+percent parent+value",
        ),
        layout=dict(
            title=dict(
                text=title,
                pad=dict(l=20),
                y=0.99,
                subtitle=dict(
                    text=subtitle,
                ),
            ),
            height=HEIGHT,
        ),
    )

    return fig


# Get the total number of incidents for a given label
def get_total_incidents(df, label, colx, coly):
    if label in df[colx].values:
        return df[df[colx] == label]["total_inc"].sum()
    return df[df[coly] == label]["total_inc"].sum()


def create_sankey(df, colx, coly, title, subtitle="", df_total=None):
    """
    Create a sankey chart
    """
    data = dict(
        type="sankey",
        node=dict(
            pad=21,
            thickness=15,
            line=dict(color="black", width=0.6),
            label=df["label"].dropna(),
            color=[colors[label] for label in df["label"] if label != ""],
            hovertemplate="Incident: %{label}<br>Number Incidents: %{value}",
        ),
        link=dict(
            source=df["source"],
            target=df["target"],
            value=df["value"],
            color=[colors[df["label"][i]] for i in df["source"]],
            hovertemplate="Source: %{source.label}<br>Target: %{target.label}<br>Number Incidents: %{value}",
        ),
    )

    # Add the total number of incidents to the labels
    for i, label in enumerate(data["node"]["label"]):
        total_incidents = get_total_incidents(df_total, label, colx, coly)
        data["node"]["label"][i] += f" ({total_incidents})"

    fig = go.Figure(
        data,
        layout=dict(
            title=dict(
                text=title,
                pad=dict(l=20),
                y=0.99,
                subtitle=dict(
                    text=subtitle,
                ),
            ),
            font_size=16,
            height=680,
        ),
    )

    return fig


def create_heatmap(df, year):
    """
    Create a heatmap chart
    """

    # get data where columns as year
    df_year = df[df.columns[df.columns.str.startswith(year)]]

    # remove year from columns
    df_year.columns = df_year.columns.str.split("_").str[1]

    fig = go.Figure(
        data=go.Heatmap(
            z=df_year,
            x=df_year.columns,
            y=df_year.index,
            colorscale="thermal",
            xgap=3,
            ygap=3,
            hovertemplate="Semaine: %{x}<br>Jour: %{y}<br>Incidents: %{z}",
        ),
        layout=dict(
            title="Qunatity of Incidents by day for the year " + year,
            xaxis_nticks=52,
            xaxis_title="Weeks",
            height=370,
            width=400,
        ),
    )

    return fig


def create_map(df_region):
    """
    Create a map chart
    """
    df_json_region = get_region()

    # convert to df
    df_json_region = pd.DataFrame(
        df_json_region.items(), columns=["region", "id_region"]
    )

    # add id_region missing in the geojson file
    df = (
        df_region.merge(df_json_region, left_on="label", right_on="region", how="right")
        .fillna(0)
        .rename(columns={"id_region_y": "id_region"})
    )

    # read file json
    with open("core/utils/ru_region.json") as file:
        polygons = json.load(file)

    fig = go.Figure(
        data=go.Choropleth(
            geojson=polygons,
            locations=df["id_region"],
            text=df["region"],
            z=df["total_inc"],
            locationmode="geojson-id",
            hoverinfo="z+text",
            colorscale="thermal",
            colorbar_title="Sabotages",
            marker_line_color="darkgray",
            marker_line_width=0.5,
        ),
    )

    fig.update_layout(
        # title_text="Geographic Distribution of Incidents on Russian Railways",
        title=dict(
            text="Geographic Distribution of Sabotages on Russian Railways",
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text="Number of Railway Sabotages by Region in Russia from 2023 to 2024",
            ),
        ),
        geo=dict(
            showframe=False,
            projection=dict(type="eckert1", scale=3.8),
            center=dict(lat=60, lon=91),
            showland=True,
            landcolor="rgb(172, 172, 172)",
            showcountries=True,
            countrycolor="rgb(204, 204, 204)",
        ),
        height=800,
        margin={
            "r": 0,
            "l": 0,
            "b": 0,
        },
    )

    return fig


def create_wordcloud(df):
    """
    Create a wordcloud chart
    """
    wordcloud = WordCloud(
        width=450,
        height=250,
        max_words=df.shape[0],
        margin=0,
        max_font_size=45,
        background_color="#0e1117",
        relative_scaling=0,
        colormap="OrRd",
    ).generate_from_frequencies(df.set_index("text")["total_inc"])

    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")

    return fig


def create_funnel(df, title, subtitle=""):
    """
    Create a funnel chart
    """
    fig = go.Figure(
        go.Funnel(
            y=df["niv"],
            x=df["count"],
            textinfo="label+value",
            marker=dict(
                color=[colors[label] for label in df["niv"]],
                line=dict(width=4, color="rgb(38, 45, 58)"),
            ),
            connector=dict(
                line=dict(color="rgb(38, 45, 58)", width=1),
                fillcolor="rgb(29, 34, 44)",
            ),
            textposition="inside",
            textfont=dict(color="white", size=16),
            insidetextanchor="middle",
            opacity=0.90,
        )
    )

    # Update the layout to modify the color between blocks
    fig.update_traces(connector=dict(line=dict(color="rgba(153, 131, 131, 0.5)")))

    fig.update_layout(
        title=dict(
            text=title,
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text=subtitle,
            ),
        ),
        height=HEIGHT,
    )

    fig.update_yaxes(visible=False)

    return fig


def create_waffle(df):
    """
    Create a waffle chart
    """
    fig = plt.figure(
        FigureClass=Waffle,
        title={
            "label": "Number of Partisans Arrested by Age Group",
            "loc": "left",
            "fontdict": {"fontsize": 10},
            "color": "white",
            "backgroundcolor": "#0e1117",
            "pad": 5,
        },
        rows=5,
        values=df["total_inc"],
        labels=[
            f"{label} ans: {value:.0f} ({value / df['total_inc'].sum() * 100:.1f}%)"
            for label, value in zip(df["label"], df["total_inc"])
        ],
        icons=["child", "person", "user-tie", "person-cane"],
        colors=[colors[label] for label in df["label"]],
        font_size=9,
        legend={
            "loc": "lower left",
            "bbox_to_anchor": (0, -0.3),
            "ncol": len(df),
            "fontsize": 4,
        },
    )
    fig.set_facecolor("#0e1117")

    return fig


def create_waterfall(df, title, subtitle="", is_group=False):
    """
    Create a waterfall chart
    """

    def categorize_age(age):
        if int(age) < 18:
            return "<18"
        elif 18 <= int(age) <= 30:
            return "18-30"
        elif 31 <= int(age) <= 50:
            return "31-50"
        else:
            return ">50"

    if is_group:

        # create age group
        df["age_group"] = df["label"].apply(categorize_age)

        fig = go.Figure()

        # add traces
        for age_group, group_df in df.groupby("age_group"):
            fig.add_trace(
                go.Waterfall(
                    name=f"Age Group {age_group}",
                    orientation="v",
                    measure=["relative"] * len(group_df),
                    x=group_df["label"],
                    y=group_df["total_inc"],
                    textposition="outside",
                    text=group_df["total_inc"],
                    hovertemplate="Arrested: %{text}",
                    hovertext=group_df["label"],
                    connector={"visible": False},
                    increasing=dict(marker=dict(color=colors[age_group])),
                )
            )

    else:
        fig = go.Figure(
            go.Waterfall(
                orientation="v",
                measure=["relative"] * len(df),
                x=df["label"],
                y=df["total_inc"],
                textposition="outside",
                text=df["total_inc"],
                connector={
                    "visible": False,
                },
                name="",
                hovertemplate="Age: %{x}<br>Arrested: %{text}",
                hovertext=df["label"],
            ),
        )

    # add x-axis data age
    fig.update_xaxes(
        title_text="Age",
        type="linear" if is_group else "log",
        showgrid=True,
        tickvals=np.arange(0, df["total_inc"].sum() + 1, 5),
    )

    fig.update_yaxes(
        title_text="Number of Partisans",
        showgrid=True,
        tickvals=np.arange(0, df["total_inc"].sum() + 1, 5),
    )

    fig.update_layout(
        title=dict(
            text=title,
            pad=dict(l=20),
            y=0.99,
            subtitle=dict(
                text=subtitle,
            ),
        ),
        height=HEIGHT,
        showlegend=False,
        font=dict(size=14),
    )

    return fig
