import dash
import dash_bootstrap_components as dbc
from dash import Dash, dcc, html

from assets.components.navbar import create_navbar

app = Dash(
    __name__,
    use_pages=True,
    title="Amazon Dashboard",
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://use.fontawesome.com/releases/v5.15.4/css/all.css",
    ],
    # suppress_callback_exceptions=True,
)


# Create the navbar
navbar = create_navbar()


# layout
app.layout = html.Div(
    # className="app-container",
    children=[
        navbar,
        dash.page_container,
    ],
)

if __name__ == "__main__":
    # app.run_server(debug=False)
    app.run(debug=True, port=8070)
