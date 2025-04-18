import dash
from dash import html, dcc

from utils.variables import DICT_CONTENT
from assets.components.cards import create_card


dash.register_page(__name__, path="/")


theme = {
    "dark": True,
    "detail": "#743800",
    "primary": "#ea7d00",
    "secondary": "#6E6E6E",
}

layout = html.Div(
    [
        # Header
        html.H1(
            "Russia-Ukraine War Analytics Hub",
            style={
                "textAlign": "center",
                "color": theme["primary"],
                "marginBottom": "30px",
                "paddingTop": "20px",
            },
        ),
        # Brief introduction
        html.Div(
            className="section-intro",
            children=[
                html.Div(
                    className="intro-header",
                    children=[
                        html.Div(
                            className="intro-content",
                            children=[
                                dcc.Markdown(
                                    """
                                Welcome to the Ukraine-Russia Conflict Data Hub. This platform aims to aggregate and analyze data related to the ongoing invasion of Ukraine by Russia.
                                """,
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
        # Cards
        html.Div(
            [
                create_card(
                    data["title"],
                    data["text"],
                    data["image"],
                    data["url"],
                    data["tags"],
                    data["color_tags"],
                )
                for key, data in DICT_CONTENT.items()
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "flexWrap": "wrap",
                "justifyContent": "center",
                "marginBottom": "100px",
            },
        ),
    ],
    className="page-content",
)
