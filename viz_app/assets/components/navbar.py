import dash
from dash import html, dcc, callback
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from utils.variables import DICT_CONTENT


def create_navbar():
    navbar = html.Div(
        [
            # Button to develop / collapse the navbar
            html.Button(
                html.I(className="fas fa-bars", id="navbar-toggle-icon"),
                id="navbar-toggle",
                className="navbar-toggle-button",
            ),
            # navbar content
            html.Div(
                [
                    # App logo or name at the top
                    html.Div(
                        [
                            html.I(
                                className="fas fa-analytics",
                                style={"fontSize": "24px"},
                            ),
                        ],
                        className="navbar-header",
                    ),
                    # Navigation links
                    html.Div(
                        [
                            html.A(
                                [
                                    html.I(
                                        className="fas fa-home",
                                    ),
                                    html.Span("Home", className="nav-text"),
                                ],
                                href="/",
                                className="nav-link",
                            ),
                        ]
                        + [
                            html.A(
                                [
                                    html.I(
                                        className=f"fas {data['icon']}",
                                    ),
                                    html.Span(data["title"], className="nav-text"),
                                ],
                                href=data["url"],
                                className="nav-link",
                                id=f"nav-link-{key}",
                            )
                            for key, data in DICT_CONTENT.items()
                        ],
                        className="navbar-nav",
                    ),
                    # Footer with GitHub link
                    html.Div(
                        className="navbar-footer",
                        children=[
                            html.A(
                                [
                                    html.I(
                                        className="fa fa-question-circle",
                                    ),
                                ],
                                href="/infos",
                                className="nav-link",
                            ),
                            html.A(
                                [
                                    html.I(
                                        className="fab fa-github",
                                    ),
                                ],
                                href="https://github.com/Kyytox/russia_ukraine_war_analytics_hub",
                                target="_blank",
                                className="nav-link",
                            ),
                        ],
                    ),
                ],
                id="navbar",
                className="navbar",
            ),
            html.Div(id="overlay", className="overlay"),
        ],
        className="navbar-container",
    )

    return navbar


@callback(
    [
        Output("navbar", "className"),
        Output("overlay", "className"),
        Output("navbar-toggle-icon", "className"),
    ],
    [Input("navbar-toggle", "n_clicks"), Input("overlay", "n_clicks")],
    [State("navbar", "className")],
)
def toggle_navbar(n1, n2, navbar_class):
    ctx = dash.callback_context
    if not ctx.triggered:
        return "navbar", "overlay", "fas fa-bars"
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if "expanded" in navbar_class:
            # Collapse the navbar
            return "navbar", "overlay", "fas fa-bars"
        else:
            # Expand the navbar
            return "navbar expanded", "overlay show", "fas fa-times"
