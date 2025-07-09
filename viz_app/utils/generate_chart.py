import pandas as pd
import numpy as np
import json

import plotly.graph_objects as go

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
            textfont=dict(size=12),
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
            textfont=dict(size=12),
            # textposition="outside",
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
                textfont=dict(size=12),
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
                textfont=dict(size=12),
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
            textfont=dict(size=12),
            textinfo="label+percent parent+value",
        ),
    )

    fig = fig_upd_layout(fig, title, subtitle)

    return fig


def get_region():
    """
    Get the region and id from the json file
    """

    # read file json
    with open("core/utils/ru_region.json") as file:
        data = json.load(file)

    # get id and name
    dict_region = {}
    for i in range(len(data["features"])):
        dict_region[data["features"][i]["properties"]["name"]] = data["features"][i][
            "id"
        ]

    # update dict
    dict_region = {
        k.replace("Moskva", "Moscow").replace("'", ""): v
        for k, v in dict_region.items()
    }

    return dict_region


def prepare_df_region(df_region):
    """
    Prepare the dataframe for the map chart
    """
    # get the total number of incidents by region
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

    return df


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


def generate_funnel(df, title, subtitle=""):
    """
    Create a funnel chart
    """
    fig = go.Figure(
        go.Funnel(
            y=df["niv"],
            x=df["count"],
            textinfo="label+value",
            marker=dict(
                color=[COLORS_RAILWAY[label] for label in df["niv"]],
                line=dict(width=4, color="rgb(38, 45, 58)"),
            ),
            connector=dict(
                line=dict(color="rgb(38, 45, 58)", width=1),
                fillcolor="rgb(29, 34, 44)",
            ),
            textposition="inside",
            textfont=dict(color="white", size=12),
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
    )

    fig.update_yaxes(visible=False)
    fig = fig_upd_layout(fig)

    return fig


def generate_waterfall(df, title, subtitle="", is_group=False):
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
                    increasing=dict(marker=dict(color=COLORS_RAILWAY[age_group])),
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
        tickvals=np.arange(int(df["label"].min()), int(df["label"].max()) + 1, 5),
    )

    fig.update_yaxes(
        title_text="Number of Partisans",
        showgrid=True,
        tickvals=np.arange(0, df["total_inc"].sum() + 1, 10),
    )

    fig.update_layout(
        title=dict(
            text=title,
            subtitle=dict(
                text=subtitle,
            ),
        ),
        showlegend=False,
        font=dict(size=14),
    )

    fig = fig_upd_layout(fig)

    return fig


# Get the total number of incidents for a given label
def get_total_incidents(df, label, colx, coly):
    if label in df[colx].values:
        return df[df[colx] == label]["total_inc"].sum()
    return df[df[coly] == label]["total_inc"].sum()


def generate_sankey(df, colx, coly, title, subtitle="", df_total=None):
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
            color=[COLORS_RAILWAY[label] for label in df["label"] if label != ""],
            hovertemplate="Incident: %{label}<br>Number Incidents: %{value}",
        ),
        link=dict(
            source=df["source"],
            target=df["target"],
            value=df["value"],
            color=[COLORS_RAILWAY[df["label"][i]] for i in df["source"]],
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
    fig = fig_upd_layout(fig)

    return fig


def generate_waffle(df):
    """
    Create a waffle chart (Heatmap Plotly)
    """

    # data
    labels = df["label"].tolist()
    values = df["total_inc"].tolist()

    # Parameters for the waffle chart
    total = sum(values)
    max_rows = 10  # Limite le nombre de lignes
    cols = int(np.ceil(total / max_rows))
    rows = int(np.ceil(total / cols))

    # Build the z matrix
    z = np.zeros((rows * cols,))
    category_ids = []
    for i, v in enumerate(values):
        category_ids.extend([i + 1] * int(v))  # +1 pour correspondre aux couleurs dict
    z[: len(category_ids)] = category_ids
    z = z.reshape((rows, cols))

    # Build the labels matrix
    d = {1: "<18", 2: "18-30", 3: "31-50", 4: ">50"}
    colorscale = [
        [0, COLORS_RAILWAY["<18"]],
        [0.25, COLORS_RAILWAY["18-30"]],
        [0.5, COLORS_RAILWAY["31-50"]],
        [0.75, COLORS_RAILWAY[">50"]],
        [1, COLORS_RAILWAY[">50"]],
    ]

    # Create customdata for hover information
    M = max(len(s) for s in labels)
    customdata = np.empty((rows, cols), dtype=f"<U{M}")
    print(customdata)
    for i in range(rows):
        for j in range(cols):
            val = z[i, j]
            customdata[i, j] = d.get(int(val), "")

    # Remove empty strings from customdata
    customdata = [[valeur for valeur in ligne if valeur != ""] for ligne in customdata]

    # Remove 0 from z
    z = [[valeur for valeur in ligne if valeur != 0.0] for ligne in z]

    # Create the heatmap figure
    fig = go.Figure(
        go.Heatmap(
            z=z,
            customdata=customdata,
            xgap=3,
            ygap=3,
            colorscale=colorscale,
            showscale=False,
            hovertemplate="%{customdata} Group<extra></extra>",
        )
    )

    fig.update_layout(
        height=rows * 80,
        title="Number of Partisans Arrested by Age Group",
        # hide xaxis and yaxis
        xaxis=dict(
            showticklabels=False,
            ticks="",
            showgrid=False,
            zeroline=False,
        ),
        yaxis=dict(
            showticklabels=False,
            ticks="",
            showgrid=False,
            zeroline=False,
        ),
        margin=dict(l=50, r=50, t=60, b=50),
        title_x=0.5,
        title_y=0.95,
    )

    fig = fig_upd_layout(fig)

    total_count = sum(values)
    for i, label in enumerate(labels):
        count = values[i]
        percentage = (count / total_count) * 100
        color = COLORS_RAILWAY[label]

        # Add invisible scatter trace for each legend item
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(size=10, color=color, symbol="square"),
                legendgroup=label,
                name=f"{label} ({int(count)} - {percentage:.1f}%)",
                hoverinfo="skip",
            )
        )

    # Update legend layout
    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
            font=dict(size=12),
            itemsizing="constant",
        )
    )

    return fig
