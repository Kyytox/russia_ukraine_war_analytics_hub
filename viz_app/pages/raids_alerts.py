import pandas as pd

import dash
from dash import Input, Output, callback, html, dcc
from utils.variables import PATH_DMT_BLOCK_SITE

from assets.components.cards import create_card


dash.register_page(__name__)


dmt_global = pd.read_parquet(f"{PATH_DMT_BLOCK_SITE}/dmt_global.parquet")


page_content = {
    "Air Raid Alert Map of Ukraine": {
        "title": "Air Raid Alert Map of Ukraine",
        "text": "Map of air raid alerts in Ukraine",
        "image": None,
        "url": "https://alerts.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
    },
    "Air-alarms.in.ua": {
        "title": "Air-alarms.in.ua",
        "text": "Statistics of air alerts in Ukraine",
        "image": None,
        "url": "https://air-alarms.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
    },
    "Ukrainian Air Raid Sirens Dataset": {
        "title": "Ukrainian Air Raid Sirens Dataset",
        "text": "GitHub repository with the Ukrainian Air Raid Sirens Dataset",
        "image": None,
        "url": "https://github.com/Vadimkin/ukrainian-air-raid-sirens-dataset",
        "tags": ["Data Sources"],
        "color_tags": ["#14aca4"],
    },
}


layout = html.Div(
    className="page-content",
    children=[
        # Header
        html.H1(
            className="page-title",
            children="Raid Alerts in Ukraine 🚨",
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
                for key, data in page_content.items()
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "flexWrap": "wrap",
                "justifyContent": "center",
            },
        ),
    ],
)
