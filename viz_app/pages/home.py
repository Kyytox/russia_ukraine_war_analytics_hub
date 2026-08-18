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
        html.Header(
            children=[
                html.H1(
                    "Russia-Ukraine War Data Hub",
                    style={
                        "textAlign": "center",
                        "color": theme["primary"],
                        "marginBottom": "30px",
                        "paddingTop": "20px",
                    },
                ),
            ]
        ),
        # Intro Section
        html.Main(
            children=[
                html.Section(
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
                                            This platform aggregates and analyzes **open data** related to the ongoing invasion of Ukraine by Russia.
                                            My mission is to provide data, visualizations, and insights about the war, and gather as much information as possible in one place.
                                            """,
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
                # Cards section
                html.Section(
                    className="section-cards",
                    children=[
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
                ),
            ],
            className="page-content",
        ),
    ],
)
