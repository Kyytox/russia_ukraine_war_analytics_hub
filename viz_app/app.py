import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from assets.components.navbar import create_navbar
from assets.components.footer import create_footer

app = Dash(
    __name__,
    use_pages=True,
    title="Ukraine War Data Hub | Open Data & Analytics on Russia-Ukraine War",
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://use.fontawesome.com/releases/v5.15.4/css/all.css",
    ],
    suppress_callback_exceptions=True,
    description="Explore open data and analytics on the Russia-Ukraine war. Access data, visualizations, the list of websites blocked in Russia, alerts related to missile raids in Ukraine, components used in the aggressor`s weapon, interactive maps related to the war. ",
    index_string="""
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            <meta name = "description" content="Explore open data and analytics on the Russia-Ukraine war. Access data, visualizations, the list of websites blocked in Russia, alerts related to missile raids in Ukraine, components used in the aggressor`s weapon, interactive maps related to the war.">
            {%favicon%}
            {%css%}
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                {%renderer%}
            </footer>
        </body>
    </html>
    """,
)

server = app.server


# Create the navbar
navbar = create_navbar()

# Footer
footer = create_footer()

# script
script = html.Script(
    """
        window.dataLayer = window.dataLayer || [];
        function gtag(){dataLayer.push(arguments);}
        gtag('js', new Date());
        gtag('config', 'G-PSVZK81FYS');
        """,
    src="https://www.googletagmanager.com/gtag/js?id=G-PSVZK81FYS",
)


# layout
app.layout = html.Div(
    children=[
        script,
        navbar,
        dash.page_container,
        footer,
    ],
)

if __name__ == "__main__":
    # app.run(debug=True, port=8503)
    app.run(debug=False, port=8501)
