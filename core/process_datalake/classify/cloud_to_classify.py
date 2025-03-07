from datetime import datetime

import pandas as pd

from prefect import flow, task

# Functions
from core.libs.utils import read_data, concat_old_new_df, save_data
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
    update_sheet_data,
)

# Variables
from core.config.paths import (
    PATH_CLASSIFY_DATALAKE,
    PATH_QUALIF_DATALAKE,
    PATH_FILTER_DATALAKE,
)

from core.config.variables import ID_EXCEL_INCIDENT_RAILWAY
from core.config.schemas import SCHEMA_QUALIF_RAILWAY, SCHEMA_EXCEL_RAILWAY


# dict_corr_schema = {
#     # "ID": "ID",
#     "IDX": "IDX",
#     "class_date_inc": "Date",
#     "class_region": "Region",
#     "class_location": "Location",
#     "class_gps": "Gps",
#     "class_dmg_eqp": "Damaged Equipment",
#     "class_inc_type": "Incident Type",
#     # "class_sabotage_success": "Sabotage Success",
#     # "class_nb_loco_dmg": "Locomotive Damaged",
#     # "class_nb_relay_dmg": "Relay Damaged",
#     "class_coll_with": "Collision With",
#     "class_prtsn_grp": "Partisans Group",
#     "class_prtsn_arr": "Partisans Arrest",
#     "class_prtsn_names": "Partisans Names",
#     "class_prtsn_age": "Partisans Age",
#     "class_app_laws": "Applicable Laws",
#     "class_sources": "Source Links",
#     "class_comments": "Comments",
# }

dict_corr_schema = {
    # "ID": "ID",
    "IDX": "IDX",
    "Date": "class_date_inc",
    "Region": "class_region",
    "Location": "class_location",
    "Gps": "class_gps",
    "Damaged Equipment": "class_dmg_eqp",
    "Incident Type": "class_inc_type",
    # "Sabotage Success": "class_sabotage_success",
    # "Locomotive Damaged": "class_nb_loco_dmg",
    # "Relay Damaged": "class_nb_relay_dmg",
    "Collision With": "class_coll_with",
    "Partisans Group": "class_prtsn_grp",
    "Partisans Arrest": "class_prtsn_arr",
    "Partisans Names": "class_prtsn_names",
    "Partisans Age": "class_prtsn_age",
    "Applicable Laws": "class_app_laws",
    "Source Links": "class_sources",
    "Comments": "class_comments",
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

    # rename columns
    df = df.rename(columns=dict_corr_schema)

    # remove jump line in sources
    df["class_sources"] = df["class_sources"].str.replace("\n", " ")

    # convert date
    df["class_date_inc"] = pd.to_datetime(
        df["class_date_inc"], errors="coerce", format="%m/%d/%Y"
    )

    return df


@task(name="Group URL ID New Classify", task_run_name="group-url-id-new-classify")
def group_url_id_new_classify(df):
    """
    Group data by IDX
    If there are multiple same IDX, group all ID and URL

    Args:
        df: DataFrame with new classify data
    """

    # add cols mul_ID, mul_url, regoup all if IDX is muultiple
    df["mul_ID"] = df.groupby("IDX")["ID"].transform(lambda x: ",".join(x))
    df["mul_url"] = df.groupby("IDX")["class_sources"].transform(lambda x: ",".join(x))

    # replace cols
    df = (
        df.drop(columns=["ID", "class_sources"])
        .rename(
            columns={
                "mul_ID": "ID",
                "mul_url": "class_sources",
            }
        )
        .drop_duplicates(["IDX"], keep="first")
        .reset_index(drop=True)
    )

    return df


@task(name="Update Qualif Data", task_run_name="update-qualif-data")
def update_qualif_data(df, df_qualif):
    """
    Update Qualif Data

    Args:
        df: DataFrame with new classify data
        df_qualif: DataFrame with qualification data
    """

    # rename cols (replace class_ to qualif_)
    df = df.rename(columns=lambda x: x.replace("class_", "qualif_"))

    # rename qualif_sources to url
    df = df.rename(columns={"qualif_sources": "url"})

    # add qualif_ia with True
    df["qualif_ia"].fillna("True", inplace=True)

    # fillna
    df["qualif_nb_loco_dmg"] = df["qualif_nb_loco_dmg"].fillna(0)
    df["qualif_nb_relay_dmg"] = df["qualif_nb_relay_dmg"].fillna(0)

    # update type with schema
    df = df.astype(SCHEMA_QUALIF_RAILWAY)

    # concat dataframes
    df_qualif = concat_old_new_df(df_qualif, df, ["ID", "IDX"])
    print(df_qualif)

    # save data
    save_data(PATH_QUALIF_DATALAKE, "qualification_railway", df_qualif)


@task(name="Update Filter Data", task_run_name="update-filter-data")
def update_filter_data(df, df_filter):
    """
    Update Filter Data

    Args:
        df: DataFrame with new classify data
        df_filter: DataFrame with qualification data
    """

    # keep ID, date, class_sources
    df = df[["ID", "date", "class_sources"]]

    # rename source_links to url
    df = df.rename(columns={"class_sources": "url"})

    # add cols
    df["text_original"] = ""
    df["text_translate"] = ""

    # add filter
    df["filter_inc_railway"] = True
    df["found_terms_railway"] = ""
    df["add_final_inc_railway"] = True

    # get cols who type bool but not in df
    cols = [
        col
        for col in df_filter.columns
        if df_filter[col].dtype == "bool" and col not in df.columns
    ]

    # add missing cols
    for col in cols:
        df[col] = False

    # get cols who start with "found_" but not in df
    cols = [
        col
        for col in df_filter.columns
        if col.startswith("found_") and col not in df.columns
    ]

    # add missing cols
    for col in cols:
        df[col] = ""

    # concat dataframes
    df_filter = concat_old_new_df(df_filter, df, ["ID"])
    print(df_filter)

    # save data
    save_data(PATH_FILTER_DATALAKE, "filter_datalake", df_filter)


@task(name="Update Classify Data", task_run_name="update-classify-data")
def update_classify_data(df, df_classify):
    """
    Update Classify Data

    Args:
        df: DataFrame with new classify data
        df_classify: DataFrame with classify data
    """

    # concat dataframes
    df_classify = (
        pd.concat([df_classify, df], ignore_index=True)
        .drop_duplicates(["IDX", "class_sources"], keep="last")
        .sort_values("class_date_inc")
        .reset_index(drop=True)
    )

    # rename cols, replace qualif_ to class_
    df_classify = df_classify.rename(columns=lambda x: x.replace("qualif_", "class_"))

    # filna
    df_classify["class_nb_loco_dmg"] = df_classify["class_nb_loco_dmg"].fillna(0)
    df_classify["class_nb_relay_dmg"] = df_classify["class_nb_relay_dmg"].fillna(0)

    # convert bool
    df_classify["class_prtsn_arr"] = df_classify["class_prtsn_arr"].replace(
        "FALSE", False
    )
    df_classify["class_prtsn_arr"] = df_classify["class_prtsn_arr"].replace(
        "TRUE", True
    )

    # regoup by IDX, concat ID with "," and class_sources with ","
    df_classify = group_url_id_new_classify(df_classify)

    # keep cols in schema
    df_classify = df_classify[SCHEMA_EXCEL_RAILWAY.keys()]
    print(df_classify)
    df_classify = df_classify.astype(SCHEMA_EXCEL_RAILWAY)
    print(df_classify)
    print(df_classify.dtypes)

    # save data
    save_data(PATH_CLASSIFY_DATALAKE, "classify_inc_railway", df_classify)


@flow(
    name="Flow Master Cloud to Classify",
    flow_run_name="flow-master-cloud-to-classify",
    log_prints=True,
)
def flow_cloud_to_classify():
    """
    Flow Master Cloud to Classify
    """

    spreadsheet_id = ID_EXCEL_INCIDENT_RAILWAY
    range_name = "Incidents Russian Railway - DATA"

    # connect to google sheet
    service = connect_google_sheet_api()

    """
    Init Data
    """
    # get data from Google Sheet
    df_excel = get_sheet_data(service, spreadsheet_id, range_name)
    df_qualif = read_data(PATH_QUALIF_DATALAKE, "qualification_railway")
    df_filter = read_data(PATH_FILTER_DATALAKE, "filter_datalake")

    """
    Process Data
    """
    # format data
    df_excel = format_columns(df_excel)

    # Split dataframe by URLs in class_sources column
    df_excel = df_excel.assign(
        class_sources=df_excel["class_sources"].str.split(",")
    ).explode("class_sources")

    # Clean whitespace from URLs
    df_excel["class_sources"] = df_excel["class_sources"].str.strip()

    # remove source links with empty string
    df_excel = df_excel[df_excel["class_sources"] != ""]

    # Reset index after exploding
    df_excel = df_excel.reset_index(drop=True)

    #
    #
    #
    #

    # merge with classify by URL to get ID
    df_excel = (
        df_excel.merge(
            df_qualif[
                [
                    "ID",
                    "url",
                    "date",
                    "qualif_ia",
                    "qualif_nb_loco_dmg",
                    "qualif_nb_relay_dmg",
                ]
            ],
            how="left",
            left_on="class_sources",
            right_on="url",
        )
        .drop(columns=["url"])
        .drop_duplicates()
    )

    # get max ID who start with "extrenal"
    df_links_extranl = df_excel[df_excel["ID"].str.startswith("external", na=False)]

    # get ID empty
    df_id_empty = df_excel[df_excel["ID"].isnull()]

    if df_id_empty.shape[0] > 0:
        if df_links_extranl.shape[0] > 0:
            max_id = (
                df_links_extranl["ID"].str.extract(r"(\d+)").astype(int).max().values[0]
            )
            print(max_id)
        else:
            max_id = 0
    else:
        print("No data to process")
        return

    # fillna ID with "external_" + increment(max_id)
    df_id_empty.loc[df_id_empty["ID"].isnull(), "ID"] = [
        f"external_{i}"
        for i in range(max_id + 1, max_id + 1 + len(df_excel[df_excel["ID"].isnull()]))
    ]

    # fillna date with class_date_inc
    df_id_empty.loc[df_id_empty["date"].isnull(), "date"] = df_id_empty[
        "class_date_inc"
    ]
    print(df_id_empty)

    #
    #
    #
    # Update Qualif Data
    update_qualif_data(df_id_empty, df_qualif)

    #
    #
    #
    # Update Filter Data
    update_filter_data(df_id_empty, df_filter)

    #
    #
    #
    # Update Classify Data
    update_classify_data(df_id_empty, df_excel)
