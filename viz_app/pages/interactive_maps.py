import pandas as pd

import dash
from dash import Input, Output, callback, html, dcc
from utils.variables import PATH_DMT_BLOCK_SITE

from assets.components.cards import create_card


dash.register_page(__name__)


dmt_global = pd.read_parquet(f"{PATH_DMT_BLOCK_SITE}/dmt_global.parquet")


page_content = {
    "card1": {
        "title": "DeepState Map of Ukraine",
        "text": "News of russia's war against Ukraine on the map.",
        "image": None,
        "url": "https://deepstatemap.live/en#6/50.0571388/32.7612305",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card2": {
        "title": "UA Control Map",
        "text": "UA Control And Units Map.",
        "image": None,
        "url": "uacontrolmap.com",
        "tags": ["Map", "Units"],
        "color_tags": ["#121096", "#aaa824"],
    },
    "card3": {
        "title": "War Mapper",
        "text": "Understand the impact of the frontline changes in Ukraine through a series of control charts.",
        "image": None,
        "url": "https://www.warmapper.org/",
        "tags": ["Map", "Graphs"],
        "color_tags": ["#121096", "#4abb15"],
    },
    "card4": {
        "title": "Eyes on Russia",
        "text": "Map draws on the database of videos, photos, satellite imagery or other media related to Russia’s invasion of Ukraine.",
        "image": None,
        "url": "https://eyesonrussia.org/",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card5": {
        "title": "Russian Military Objects Are in Range of ATACMS",
        "text": "Map allows users to inspect 225 known Russian military objects in Russia that are in range of Ukrainian ATACMS.",
        "image": None,
        "url": "https://storymaps.arcgis.com/stories/8b060c46ee6f49908f9fb415ad23051c",
        "tags": ["Map"],
        "color_tags": ["#121096"],
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
