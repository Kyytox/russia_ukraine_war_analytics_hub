# Process filter
# - Read data from clean
# - Apply filter to df_filter (according to col filter_theme)
# - Save data


from prefect import flow, task

from core.libs.utils import read_data, save_data

# Variables
from core.utils.variables import (
    path_twitter_clean,
    path_twitter_filter,
)


@task(name="Filter incidents railway")
def filter_incidents_railway(df):
    """
    Apply filter
    Get incident_railway in col filter_theme
    add cols

    Args:
        df: dataframe with filter data

    Returns:
        Dataframe with filter data
    """

    # filter according to filter_theme
    df = df[df["filter_theme"] == "incident_railway"]
    print(f"Data filtered: {df.shape}")

    # remove col filter_theme
    df = df.drop(columns=["filter_theme"])

    return df


@flow(name="Process filter", log_prints=True)
def job_twitter_filter():
    """
    Process filter
    """

    # read data from clean
    df_to_filter = read_data(path_twitter_clean, "twitter")

    # apply filter
    df = filter_incidents_railway(df_to_filter)

    # save data
    save_data(path_twitter_filter, "incidents_railway", df=df)
