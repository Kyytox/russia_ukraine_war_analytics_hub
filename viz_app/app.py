import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from assets.components.navbar import create_navbar
from assets.components.footer import create_footer

app = Dash(
    __name__,
    use_pages=True,
    title="Amazon Dashboard",
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://use.fontawesome.com/releases/v5.15.4/css/all.css",
    ],
    suppress_callback_exceptions=True,
)

server = app.server


# Create the navbar
navbar = create_navbar()

# Footer
footer = create_footer()


# layout
app.layout = html.Div(
    children=[
        navbar,
        dash.page_container,
        footer,
    ],
)

if __name__ == "__main__":
    app.run(debug=True, port=8501)
