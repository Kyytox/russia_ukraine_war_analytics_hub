# Process: Extract incident railway
# - Get source incident railway from google sheet
# - Check if the region is correct and exists in the list for graph Map
# - Check missing values in specific columns
# - if one of the previous steps fails, don't save the data
# - Save data


from prefect import flow, task
from prefect.states import Completed, Failed

# Variables
from core.utils.variables import path_dw_sources, id_excel_incident_railway

# Functions
from core.libs.utils import save_data, get_regions_geojson
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


@task(name="Check regions")
def check_regions(df, dict_region):
    """
    Check if the region is correct and exists in the list for graph Map
    """

    # get incorrect region
    list_incorrect = []
    for region in df["Region"].unique():
        if region not in dict_region.keys():
            list_incorrect.append(region)

    if len(list_incorrect) > 0:
        print(f"Region incorrect: {list_incorrect}")
        return Failed(message=f"Region incorrect: {list_incorrect}")

    return Completed(message="Region OK")


@task(name="Check missing values")
def check_missing_values(df):
    """
    Check missing values in specific columns
    """

    list_cols = [
        "Date",
        "Region",
        "Damaged Equipment",
        "Incident Type",
        "Source Links",
    ]

    # check missing values
    for col in list_cols:
        if df[col].isnull().sum() > 0:
            print(f"Column {col} has missing values {df[col].isnull().sum()}")
            return Failed(message=f"Column {col} has missing values")
        else:
            print(f"Column {col} OK")

    # check missing values in partisans_group if incident_type is Sabotage
    if df[df["Incident Type"] == "Sabotage"]["Partisans Group"].isnull().sum() > 0:
        print("Column partisans_group has missing values")
        return Failed(message="Column partisans_group has missing values")
    else:
        print("Column partisans_group OK")

    # if prtisans_group is "No affiliation" check if "Partisans Reward	Partisans Age	Partisans Arrest	Partisans Names	Applicable Laws" are not null
    if (
        df[df["Partisans Group"] == "No affiliation"][
            [
                "Partisans Reward",
                "Partisans Age",
                "Partisans Arrest",
                "Partisans Names",
                "Applicable Laws",
            ]
        ]
        .isnull()
        .all()
        .all()
    ):
        print()
        # return Failed(
        #     message="If Partisans Group is No affiliation, columns must be filled"
        # )
    else:
        print("Columns OK if Partisans Group is No affiliation")

    # if incident type is "Collision" check if "Collision With" is not null
    if df[df["Incident Type"] == "Collision"]["Collision With"].isnull().sum() > 0:
        print("Column coll_with has missing values")
        return Failed(message="Column coll_with has missing values")
    else:
        print("Column coll_with OK")

    return Completed(message="No missing values")


@flow(name="Extract incident railway", log_prints=True)
def job_extract_incident_railway():
    """
    Get source incident railway
    From google sheet
    """
    spreadsheet_id = id_excel_incident_railway
    range_name = "Incidents Russian Railway - DATA"

    # connect to google sheet
    service = connect_google_sheet_api()

    # get data from google sheet
    df = get_sheet_data(service, spreadsheet_id, range_name)
    print(df)

    # check region
    dict_region = get_regions_geojson()

    # check region
    state_reg = check_regions(df, dict_region)

    # Check missing values
    state_miss = check_missing_values(df)

    if state_miss.is_failed() or state_reg.is_failed():
        return state_miss, state_reg

    # save data
    save_data(path_dw_sources, "incidents_railway", df=df)
