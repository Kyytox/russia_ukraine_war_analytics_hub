from utils.variables_charts import (
    TITLE_FONT_SIZE,
    TITLE_COLOR,
    SUBTITLE_FONT_SIZE,
    SUBTITLE_COLOR,
    PAPER_BGCOLOR,
    PLOT_BGCOLOR,
    TEXT_COLOR,
    GRID_COLOR,
)


# Function to standardize the style of the figures
def fig_upd_layout(
    fig,
    title=None,
    subtitle=None,
    xgrid=True,
    ygrid=True,
    margin=dict(l=0, r=20, t=80, b=60),
    **kwargs
):

    fig.update_layout(
        font=dict(color=TEXT_COLOR, size=12),
        paper_bgcolor=PAPER_BGCOLOR,
        plot_bgcolor=PLOT_BGCOLOR,
        yaxis=dict(
            showgrid=ygrid,
            gridcolor=GRID_COLOR,
            zeroline=False,
            ticks="outside",
            ticklen=0,
        ),
        xaxis=dict(
            showgrid=xgrid,
            gridcolor=GRID_COLOR,
            zeroline=False,
        ),
        # margin=dict(l=60, r=40, t=80, b=180),
        margin=margin,
        **kwargs
    )

    if title is not None:
        fig.update_layout(
            title=dict(
                text=title,
                subtitle=dict(
                    text=subtitle,
                    font=dict(color=SUBTITLE_COLOR, size=SUBTITLE_FONT_SIZE),
                ),
                font=dict(
                    color=TITLE_COLOR,
                    size=TITLE_FONT_SIZE,
                ),
                xanchor="center",
                x=0.5,
                yanchor="bottom",
                y=0.96,
            ),
        )

    return fig
