import dash
from dash import html, dcc

from utils.variables import DICT_CONTENT
from assets.components.cards import create_card


dash.register_page(__name__)


theme = {
    "dark": True,
    "detail": "#743800",
    "primary": "#ea7d00",
    "secondary": "#6E6E6E",
}

layout = html.Div(
    className="page-content",
    children=[
        # Header
        html.H1("Informations"),
        # Brief introduction
        html.Div(
            className="section-infos",
            children=[
                # Objectives
                html.Div(
                    className="infos-content",
                    children=[
                        html.H2(
                            "Project Objectives",
                            className="infos-title",
                        ),
                        html.Div(
                            className="infos-text",
                            children=[
                                dcc.Markdown(
                                    """
                                - **Data Accessibility:** Provide access to a wide range of data related to the conflict, ensuring it is easily accessible to researchers, policymakers, and the public.
                                """,
                                ),
                                dcc.Markdown(
                                    """
                                - **Data Summarization:** Summarize complex datasets through intuitive graphs and visualizations to make the information more digestible and understandable.
                                """,
                                ),
                            ],
                        ),
                    ],
                ),
                # Additional Information
                html.Div(
                    className="infos-content",
                    children=[
                        html.H2(
                            "About the Project",
                            className="infos-title",
                        ),
                        html.Div(
                            className="infos-text",
                            children=[
                                dcc.Markdown(
                                    """
                                This project is an individual effort led by a data engineer. I am committed to maintaining the accuracy and integrity of the data while ensuring it is presented in a clear manner.
                                """,
                                ),
                            ],
                        ),
                    ],
                ),
                # Contact
                html.Div(
                    className="infos-content",
                    children=[
                        html.H2(
                            "Contact",
                            className="infos-title",
                        ),
                        html.Div(
                            className="infos-text",
                            children=[
                                dcc.Markdown(
                                    """
								If you have any questions or suggestions of data, [send me an message on X](https://twitter.com/Kytox_).
								""",
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
)
