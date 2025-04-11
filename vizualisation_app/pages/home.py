import dash
from dash import html, dcc

from variables import DICT_CONTENT
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
            },
        ),
    ],
    className="page-content",
)
