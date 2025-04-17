import dash
from dash import Dash, Input, Output, callback, html, dcc
import dash_bootstrap_components as dbc


def box_chart(id, fig={}, width=None, height=None):
    """Generate figure cards for the layout"""
    return html.Div(
        className="div-figure-chart",
        children=dcc.Loading(
            dcc.Graph(
                id=id,
                className="chart",
                figure=fig,
                style={"height": height},
                responsive=True,
            ),
            type="circle",
            color="#f79500",
        ),
        style={"width": width},
    )


def figure_chart_2(fig, width, height):
    """Generate figure cards for the layout"""
    return html.Div(
        className="div-figure-chart",
        children=dcc.Loading(
            dcc.Graph(
                id="fig",
                figure=fig,
                className="chart",
                style={"height": height},
                responsive=True,
            ),
            type="circle",
            color="#f79500",
        ),
        style={"width": width},
    )
