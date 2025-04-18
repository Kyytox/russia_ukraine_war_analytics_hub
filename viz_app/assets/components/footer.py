from dash import html


def create_footer():
    """
    Creates a reusable footer component with a credit message and GitHub link.

    Returns:
        dash.html.Div: A footer div component
    """
    return html.Footer(
        className="footer",
        children=[
            html.Div(
                className="footer-content",
                children=[
                    html.Span("Developed by "),
                    html.A(
                        "Kytox",
                        href="https://github.com/Kyytox",
                        target="_blank",
                    ),
                    html.Span(", Slava Ukraini!"),
                ],
            )
        ],
    )
