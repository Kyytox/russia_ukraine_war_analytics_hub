import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from variables import DICT_CONTENT


def create_navbar():
    navbar = html.Div(
        [
            # Button to develop / collapse the sidebar
            html.Button(
                html.I(className="fas fa-bars", id="sidebar-toggle-icon"),
                id="sidebar-toggle",
                className="sidebar-toggle-button",
            ),
            # Sidebar content
            html.Div(
                [
                    # App logo or name at the top
                    html.Div(
                        html.I(
                            className="fas fa-analytics",
                            style={"fontSize": "24px"},
                        ),
                        className="sidebar-header",
                    ),
                    # Navigation links
                    html.Div(
                        [
                            # home link (élément fixe)
                            html.A(
                                [
                                    html.I(
                                        className="fas fa-home",
                                        style={"marginRight": "15px"},
                                    ),
                                    html.Span("Home", className="nav-text"),
                                ],
                                href="/",
                                className="nav-link",
                            ),
                        ]
                        + [
                            # other pages (compréhension de liste)
                            html.A(
                                [
                                    html.I(
                                        className=f"fas {data['icon']}",
                                        style={"marginRight": "15px"},
                                    ),
                                    html.Span(data["title"], className="nav-text"),
                                ],
                                href=data["url"],
                                className="nav-link",
                            )
                            for key, data in DICT_CONTENT.items()
                        ],
                        className="sidebar-nav",
                    ),
                    # Footer or additional links (git, contact, etc.)
                    html.Div(
                        [
                            html.A(
                                [
                                    html.I(
                                        className="fab fa-github",
                                        style={"marginRight": "10px"},
                                    ),
                                    html.Span("GitHub", className="nav-text"),
                                ],
                                href="https://github.com/Kyytox/russia_ukraine_war_analytics_hub",
                                target="_blank",
                                className="nav-link",
                            ),
                        ],
                        className="sidebar-footer",
                    ),
                ],
                id="sidebar",
                className="sidebar",
            ),
            # Add overlay for mobile view when sidebar is expanded
            html.Div(id="overlay", className="overlay"),
        ],
        className="navbar-container",
    )

    return navbar


def init_navbar_callbacks(app):
    @app.callback(
        [
            Output("sidebar", "className"),
            Output("overlay", "className"),
            Output("sidebar-toggle-icon", "className"),
        ],
        [Input("sidebar-toggle", "n_clicks"), Input("overlay", "n_clicks")],
        [State("sidebar", "className")],
    )
    def toggle_sidebar(n1, n2, sidebar_class):
        ctx = dash.callback_context
        if not ctx.triggered:
            return "sidebar", "overlay", "fas fa-bars"
        else:
            button_id = ctx.triggered[0]["prop_id"].split(".")[0]

            if "expanded" in sidebar_class:
                # Collapse the sidebar
                return "sidebar", "overlay", "fas fa-bars"
            else:
                # Expand the sidebar
                return "sidebar expanded", "overlay show", "fas fa-times"
