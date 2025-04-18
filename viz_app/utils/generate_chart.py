import pandas as pd
import plotly.graph_objects as go

import dash
from dash import Dash, dcc, html, Input, Output, callback

from dash_holoniq_wordcloud import DashWordcloud


from utils.utils_charts import fig_upd_layout


from utils.variables_charts import (
    COLORS_RAILWAY,
    COLORS_COMP_WEAPONS,
    PAPER_BGCOLOR,
    PLOT_BGCOLOR,
    MONTHS,
    CORNER_RADIUS,
)


def get_colors(elem, colors):

    # print(elem)

    if str(colors).startswith("#"):
        return colors
    else:
        if "2022" in str(elem):
            return colors["2022"]
        elif "2023" in str(elem):
            return colors["2023"]
        elif "2024" in str(elem):
            return colors["2024"]
        elif "2025" in str(elem):
            return colors["2025"]
        else:
            return colors[elem] if elem in colors else "olive"


def generate_pie(title, subtitle, labels, values, center_txt, dict_colors, **kwargs):
    """
    Generate a pie chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        labels (list): Labels for the pie chart.
        values (list): Values for the pie chart.
        center_txt (str): Center text for the pie chart.
        dict_colors (dict): Dictionary of colors for the pie chart.

    Returns:
        plotly.graph_objects.Figure: Pie chart figure.
    """
    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            textposition="auto",
            hole=0.45,
            marker=dict(
                colors=[get_colors(elem, dict_colors) for elem in labels],
                line=dict(width=0.4, color="#0E1117"),
            ),
            textinfo=f"label+percent+value",
            hoverinfo="label+percent+value",
            title=dict(
                text=center_txt,
                font_size=17,
            ),
            **kwargs,
        ),
        layout=dict(showlegend=False),
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_bar(title, subtitle, x_, y_, text, colors, **kwargs):
    """
    Generate a bar chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        x_ (list): x-axis values for the bar chart.
        y_ (list): y-axis values for the bar chart.
        text (list): Text labels for the bars.
        colors (dict): Dictionary of colors for the bars.
        **kwargs: Additional arguments for the bar chart.

    Returns:
        plotly.graph_objects.Figure: Bar chart figure.
    """

    fig = go.Figure(
        go.Bar(
            x=x_,
            y=y_,
            marker=dict(
                color=[get_colors(elem, colors) for elem in x_],
                cornerradius=CORNER_RADIUS,
                line=dict(width=0.3, color="#171718"),
            ),
            text=text,
            textposition="outside",
            # hoverinfo="y+text",
            **kwargs,
        )
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_stacked_bar(
    title, subtitle, df, x_, y_, colors=None, start_col=1, **kwargs
):
    """
    Generate a stacked bar chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        df (pd.DataFrame): DataFrame containing the data for the stacked bar chart.
        x_ (str): x-axis column name.
        y_ (str): y-axis column name.
        colors (dict): Dictionary of colors for the bars.
        start_col (int): Starting column index for stacking.
        **kwargs: Additional arguments for the bar chart.

    Returns:
        plotly.graph_objects.Figure: Stacked bar chart figure.
    """

    fig = go.Figure(
        data=[
            go.Bar(
                x=df[x_] if x_ else df[value],
                y=df[y_] if y_ else df[value],
                name=value,
                marker=dict(
                    color=get_colors(value, colors) if colors else None,
                    cornerradius=CORNER_RADIUS,
                    line=dict(width=0.3, color="#171718"),
                ),
                text=df[value],
                textposition="auto",
                **kwargs,
            )
            for value in df.columns[start_col:]
        ],
    )

    fig.update_layout(
        barmode="stack",
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_line(
    title, subtitle, df, col_x, mode="lines+markers+text", fill="tozeroy"
):
    """
    Generate a line chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        df (pd.DataFrame): DataFrame containing the data for the line chart.
        col_x (str): x-axis column name.
        mode (str): Mode for the line chart.
        fill (str): Fill mode for the line chart.
        **kwargs: Additional arguments for the line chart.

    Returns:
        plotly.graph_objects.Figure: Line chart figure.
    """
    # sort Months
    if "month" == col_x:
        df[col_x] = pd.Categorical(df[col_x], categories=MONTHS, ordered=True)
        df = df.sort_values(col_x)

    fig = go.Figure(
        data=[
            go.Scatter(
                x=df[col_x],
                y=df[value],
                mode=mode,
                fill=fill,
                marker=dict(color=COLORS_RAILWAY[value]),
                text=df[value],
                textposition="top left",
                name=value,
                hovertemplate="Incidents: %{y}<br>Month: %{x}",
            )
            for value in df.columns[1:]
        ],
        layout=dict(
            yaxis_title="Number Incidents",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1,
                xanchor="right",
                x=1,
            ),
        ),
    )

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

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_heatmap(title, subtitle, x_, y_, z_, colors="thermal", **kwargs):
    """
    Generate a heatmap chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        x_ (list): x-axis values for the heatmap.
        y_ (list): y-axis values for the heatmap.
        z_ (list): z-axis values for the heatmap.
        colors (str): Colorscale for the heatmap.
        **kwargs: Additional arguments for the heatmap.

    Returns:
        plotly.graph_objects.Figure: Heatmap chart figure.
    """

    fig = go.Figure(
        go.Heatmap(
            x=x_,
            y=y_,
            z=z_,
            colorscale=colors,
            colorbar=dict(
                thicknessmode="pixels",
                thickness=15,
                lenmode="pixels",
                len=200,
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.0,
            ),
            **kwargs,
        )
    )
    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_treemap(title, subtitle, df, colors, **kwargs):
    """
    Generate a treemap chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        df (pd.DataFrame): DataFrame containing the data for the treemap.
        colors (dict): Dictionary of colors for the treemap.
        **kwargs: Additional arguments for the treemap chart.

    Returns:
        plotly.graph_objects.Figure: Treemap chart figure.
    """

    fig = go.Figure(
        go.Treemap(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            marker=dict(
                cornerradius=CORNER_RADIUS,
                colors=[get_colors(elem, colors) for elem in df["label"]],
            ),
            textinfo="label+percent parent+value",
            branchvalues="total",
            textfont=dict(size=13),
        ),
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_treemap2(title, subtitle, ids_, labels_, parents_, values_, colors):
    """
    Generate a treemap chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        ids_ (list): IDs for the treemap.
        labels_ (list): Labels for the treemap.
        parents_ (list): Parent IDs for the treemap.
        values_ (list): Values for the treemap.

    Returns:
        plotly.graph_objects.Figure: Treemap chart figure.
    """

    # df["color"] = df["label"].apply(lambda x: get_color(x, COLORS_COMP_WEAPONS))

    fig = go.Figure(
        go.Treemap(
            ids=ids_,
            labels=labels_,
            parents=parents_,
            values=values_,
            marker=dict(
                cornerradius=CORNER_RADIUS,
                colors=colors,
            ),
            marker_colorscale="solar",
            textinfo="label+percent parent+value",
            branchvalues="total",
            textfont=dict(size=13),
        ),
    )
    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_sunburst(title, subtitle, df, colors, **kwargs):
    """
    Generate a sunburst chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        df (pd.DataFrame): DataFrame containing the data for the sunburst chart.
        colors (dict): Dictionary of colors for the sunburst chart.
        **kwargs: Additional arguments for the sunburst chart.

    Returns:
        plotly.graph_objects.Figure: Sunburst chart figure.
    """

    df["color"] = df["label"].apply(lambda x: get_colors(x, colors))

    fig = go.Figure(
        go.Sunburst(
            ids=df["id"],
            labels=df["label"],
            parents=df["parent"],
            values=df["value"],
            branchvalues="total",
            marker=dict(
                colors=df["color"],
                line=dict(width=0.4, color="#0E1117"),
            ),
            textinfo="label+percent parent+value",
        ),
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_map(title, subtitle, polygons, loc, text_, z_, **kwargs):
    """
    Generate a map chart.

    Args:
        title (str): Title of the chart.
        subtitle (str): Subtitle of the chart.
        polygons (dict): GeoJSON polygons for the map.
        loc (list): Locations for the map.
        text_ (list): Text labels for the map.
        z_ (list): Values for the map.
        **kwargs: Additional arguments for the map chart.

    Returns:
        plotly.graph_objects.Figure: Map chart figure.
    """

    fig = go.Figure(
        data=go.Choropleth(
            geojson=polygons,
            locations=loc,
            text=text_,
            z=z_,
            hoverinfo="z+text",
            colorscale="Sunset",
            marker_line_color="darkgray",
            marker_line_width=0.5,
            **kwargs,
        ),
    )

    fig.update_geos(
        projection_type="natural earth",
        showcountries=True,
        countrycolor="#242424",
        showocean=True,
        oceancolor="#302c2c",
        showland=True,
        landcolor="#5f5f5f",
        showframe=False,
        framecolor="#1f1f1f",
        bgcolor="#302c2c",
        showlakes=False,
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def generate_wordcloud(df):
    """
    Create a wordcloud chart
    """
    security_data = []
    for _, row in df.iterrows():
        text = row.iloc[0]  # First column (text or label)
        value = row.iloc[1]  # Second column (frequency or value)
        security_data.append([text, value])

    fig = DashWordcloud(
        id="wordcloud",
        list=security_data,
        width=500,
        height=500,
        gridSize=16,
        color="#ea7d00",
        backgroundColor="#302c2c",
        shuffle=True,
        rotateRatio=0.5,
        # shrinkToFit=True,
        shape="square",
    )
