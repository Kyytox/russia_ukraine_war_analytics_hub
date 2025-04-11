from dash import html


def create_card(title, text, image, url, tags, color_tags):
    return html.A(
        className="card",
        children=[
            (
                html.Img(
                    className="card-image",
                    src=image,
                )
                if image
                else None
            ),
            html.H3(className="card-title", children=title),
            html.P(className="card-text", children=text),
            html.Div(
                children=[
                    html.Span(
                        className="card-tags",
                        children=tag,
                        style={
                            "backgroundColor": color_tags[i],
                        },
                    )
                    for i, tag in enumerate(tags)
                ]
            ),
        ],
        href=url,
    )
