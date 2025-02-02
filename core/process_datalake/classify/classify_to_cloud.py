# Process
# - get csv in Google Drive
# - get classify data
# - format data for update in Google Drive
# - Save it for archive
# - update data classified to Google Drive
#

from datetime import datetime

from prefect import flow, task

# Functions
from core.libs.utils import read_data
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from core.config.paths import PATH_CLASSIFY_DATALAKE

from core.config.variables import (
    ID_EXCEL_INCIDENT_RAILWAY,
)


dict_corr_schema = {
    "IDX": "IDX",
    "class_date_inc": "Date",
    "class_region": "Region",
    "class_location": "Location",
    "class_gps": "Gps",
    "class_dmg_eqp": "Damaged Equipment",
    "class_inc_type": "Incident Type",
    "class_sabotage_success": "Sabotage Success",
    "class_nb_loco_dmg": "Count Locomotive damaged",
    "class_nb_relay_dmg": "Count relay damaged",
    "class_coll_with": "Collision With",
    "class_prtsn_grp": "Partisans Group",
    "class_prtsn_arr": "Partisans Arrest",
    "class_prtsn_age": "Partisans Age",
    "class_prtsn_names": "Partisans Names",
    "class_app_laws": "Applicable Laws",
    "class_sources": "Source Links",
    "class_comments": "Comment",
}


@task(name="Format Columns", task_run_name="format-columns")
def format_columns(df):
    """
    Format Columns

    Args:
        df: dataframe

    Returns:
        Dataframe with formatted columns
    """
    # remove ID
    df = df.drop(columns="ID")

    # update columns name
    df = df.rename(columns=dict_corr_schema)

    # convert date to str
    df["Date"] = df["Date"].dt.strftime("%m/%d/%Y")

    # convert Int to str
    df["Count Locomotive damaged"] = df["Count Locomotive damaged"].astype(str)
    df["Count relay damaged"] = df["Count relay damaged"].astype(str)
    df["Partisans Age"] = df["Partisans Age"].astype(str)

    # replace values
    df = df.replace("0", "")
    df = df.replace("None", None)

    # bool to str (Yes/"")
    df["Partisans Arrest"] = df["Partisans Arrest"].apply(
        lambda x: "Yes" if x else None
    )

    # reorder cols
    df = df[dict_corr_schema.values()]


@flow(
    name="Flow Master Classify to Cloud",
    flow_run_name="flow-master-classify-to-cloud",
    log_prints=True,
)
def flow_classify_to_cloud():
    """
    Classify to Cloud
    """

    spreadsheet_id = ID_EXCEL_INCIDENT_RAILWAY
    range_name = "Incidents Russian Railway - DATA"
    today = datetime.today().strftime("%Y_%m_%d_%H_%M_%S")

    # connect to google sheet
    service = connect_google_sheet_api()

    # get data from Google Sheet
    df_excel_final = get_sheet_data(service, spreadsheet_id, range_name)
    print(df_excel_final)

    # get classify data
    df_classify = read_data(PATH_CLASSIFY_DATALAKE, "classify_inc_railway")

    # Compare data
    if df_excel_final.shape[0] > df_classify.shape[0]:
        print("WARNING: Excel Final has more data than CLassify !")
        return

    # save Ecel Final
    df_excel_final.to_csv(f"{PATH_CLASSIFY_DATALAKE}/old_excel_{today}.csv")

    # format data
    df_classify = format_columns(df_classify)

    try:
        update_sheet_data(service, spreadsheet_id, range_name, df_classify)
    except Exception as e:
        print("Retry Update", e)
        # connect to google sheet
        service = connect_google_sheet_api()
        update_sheet_data(service, spreadsheet_id, range_name, df_classify)
