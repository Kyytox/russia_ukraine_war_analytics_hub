import pandas as pd

import dash
from dash import Dash, html, dcc
import dash_vega_components as dvc

import plotly.graph_objects as go

from assets.components.warning_sources import warning_sources

from variables import path_dmt_block_site

dash.register_page(__name__)


dmt_global = pd.read_parquet(f"{path_dmt_block_site}/dmt_global.parquet")
dmt_by_date = pd.read_parquet(f"{path_dmt_block_site}/dmt_by_date.parquet")
dmt_by_metrics_over_time = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_metrics_over_time.parquet"
)
dmt_by_authority_category = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_authority_category.parquet"
)
dmt_by_country_category = pd.read_parquet(
    f"{path_dmt_block_site}/dmt_by_country_by_category.parquet"
)

dmt_all_data = pd.read_parquet(f"{path_dmt_block_site}/dmt_all_data.parquet")


# List of top 30 country domain
LIST_TOP_COUNTRY = (
    dmt_global[dmt_global["type_metric"] == "by_country_domain"]
    .sort_values(by="count", ascending=False)
    .metric.tolist()
)

# List of top banning authority
list_ban_authority = dmt_global[dmt_global["type_metric"] == "by_banning_authority"][
    "metric"
].unique()


# DF TREE MAP
# keep only the top 6 country
df_tree = dmt_by_country_category[
    dmt_by_country_category["country_domain"].isin(LIST_TOP_COUNTRY[:6])
]

markdown_text = """
The data has been collected from **[Websites Blocked in Russia Since February 2022](https://www.top10vpn.com/research/websites-blocked-in-russia/)**

This page is a graphical representation of the data.
"""


layout = html.Div(
    className="page-content",
    children=[
        html.H1(
            className="page-title",
            children="Blocked Websites in Russia 🚫",
        ),
        warning_sources(markdown_text),
    ],
)
