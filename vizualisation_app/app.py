# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

from assets.components.navbar import create_navbar, init_navbar_callbacks

app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://use.fontawesome.com/releases/v5.15.4/css/all.css",
        "/assets/css/main.css",
        "/assets/css/navbar.css",
        "/assets/css/cards.css",
    ],
)

# Create the navbar
navbar = create_navbar()


# Layout Main
app.layout = html.Div(
    [
        html.Div(
            [navbar, dash.page_container], id="main-content", className="main-content"
        ),
    ]
)

# Initialize the callbacks for the navbar
init_navbar_callbacks(app)

if __name__ == "__main__":
    app.run(debug=True)
