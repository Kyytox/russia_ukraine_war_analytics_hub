from dash import html, dcc


def warning_sources(markdown_text):

    return html.Div(
        className="warning-data-source",
        children=[
            html.I(className="fas fa-exclamation-triangle"),
            dcc.Markdown(children=markdown_text),
        ],
    )
