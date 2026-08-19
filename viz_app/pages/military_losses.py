import pandas as pd

import dash
from dash import Input, Output, callback, html, dcc
from utils.variables import PATH_DMT_BLOCK_SITE

from assets.components.cards import create_card

dash.register_page(__name__)


page_content = {
    "Kaggle Russia Ukraine War": {
        "title": "Equipment losses & Death Toll & Military Wounded & Prisoner of War of russians",
        "text": "This dataset describes Equipment Losses & Death Toll & Military Wounded & Prisoner of War of russians in 2022 Ukraine russia War (hosted on kaggle).",
        "image": "https://storage.googleapis.com/kaggle-datasets-images/1967621/3472411/2b2c4ecb4946701c3876d29c7923ebb1/dataset-cover.png?t=2022-04-16-14-18-08",
        "url": "https://www.kaggle.com/datasets/piterfm/2022-ukraine-russian-war",
        "tags": ["Data Sources"],
        "color_tags": ["#14aca4"],
    },
    "UA losses": {
        "title": "UA losses",
        "text": "This website maintains a list of Ukrainian soldiers killed in the current war, on the basis of public reports of deaths.",
        "image": "https://ualosses.org/static/img/logo_100.png",
        "url": "https://ualosses.org/en/soldiers/",
        "tags": ["Map", "Graphs"],
        "color_tags": ["#121096", "#4abb15"],
    },
    "Russian Losses in Ukraine": {
        "title": "Russian Losses in Ukraine",
        "text": "Daily-updated tracker of Russian personnel and equipment losses, from official reports of the Ministry of Defense of Ukraine.",
        "image": "https://www.ukrainewarlosses.com/favicon-180x180.png",
        "url": "https://www.ukrainewarlosses.com/",
        "tags": ["Data Sources"],
        "color_tags": ["#14aca4"],
    },
    "Russian Casualties in Ukraine": {
        "title": "Russian Casualties in Ukraine",
        "text": "Loss data from 02/24/2022, aggregated per day / week / month.",
        "image": None,
        "url": "https://russian-casualties.in.ua/",
        "tags": ["Data Sources", "Graphs"],
        "color_tags": ["#14aca4", "#4abb15"],
    },
    "Russo-Ukrainian Warspotting": {
        "title": "Russo-Ukrainian Warspotting",
        "text": "WarSpotting is a database of documented material losses during Russian invasion in Ukraine. We use photo evidences found accross open sources on the web such as social media as proofs.",
        "image": None,
        "url": "https://ukr.warspotting.net/",
        "tags": ["Map", "Graphs"],
        "color_tags": ["#121096", "#4abb15"],
    },
}


layout = html.Div(
    className="page-content",
    children=[
        # Header
        html.H1(
            className="page-title",
            children="Military Losses in Ukraine 🪖",
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
