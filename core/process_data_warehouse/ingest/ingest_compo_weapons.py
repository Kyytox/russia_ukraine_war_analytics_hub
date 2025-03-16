# Process: Extract incident railway
# - Get source incident railway from google sheet
# - Check if the region is correct and exists in the list for graph Map
# - Check missing values in specific columns
# - if one of the previous steps fails, don't save the data
# - Save data


import datetime
import pandas as pd

import requests
from bs4 import BeautifulSoup

from prefect import flow, task
from prefect.states import Completed, Failed

# Variables
from core.config.paths import PATH_DWH_SOURCES

# from core.config.variables import ID_EXCEL_INCIDENT_RAILWAY
# from core.config.dwh_corresp_schema import CORRESP_SCHEMA_INCIDENT_RAILWAY

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    create_artifact,
    upd_data_artifact,
)


def gestion_artifact(start_date, end_date, count_new_data):
    """
    Gestion artifact

    Args:
        start_date: start date
        end_date: end date
        count_new_data: count new data
    """
    upd_data_artifact(
        f"Start date extraction:",
        f"{start_date}",
    )
    upd_data_artifact(
        f"End date extraction:",
        f"{end_date}",
    )
    upd_data_artifact(
        f"New data extract:",
        f"{count_new_data}",
    )
    create_artifact("dwh-subflow-ingestion-compo-weapons-artifact")


@task(
    name="Get pagination",
    task_run_name="task-get-pagination",
)
def get_pagination(page_content):
    """
    Get if last page exists
    """

    # get the pagination
    pagination = page_content.find("ul", class_="pagination")

    # get class of last elements li
    last_page = " ".join(pagination.find_all("li")[-1].get("class"))

    # if last page exists
    if "disabled" in last_page:
        return False
    else:
        return True


@task(
    name="Add types weapons and equipment",
    task_run_name="task-add-types-weapons-equipment",
)
def add_types_weapons_equipment(df):
    """
    Add types weapons and equipment
    Take new data when merging (_y)

    Args:
        df: DataFrame

    Returns:
        df: DataFrame with types weapons and equipment
    """
    # read iles categories
    df_categories = pd.read_csv(
        f"{PATH_DWH_SOURCES}/components_weapons_types.csv",
    )
    df_categories = df_categories.drop(columns=["Unnamed: 4", "weapon_country"])

    # merge with categories
    df = df.merge(df_categories, on="weapon", how="left")

    # remove cols _x , replace _y by ""
    df = df.drop(columns=["type_equipment_x", "weapon_type_x"])
    df = df.rename(
        columns={
            "type_equipment_y": "type_equipment",
            "weapon_type_y": "weapon_type",
        }
    )

    return df


@task(
    name="Check if weapons are in types",
    task_run_name="task-check-weapons-in-types",
)
def check_weapons_in_types(df):
    """
    Check if weapons are in types

    Args:
        df: DataFrame

    Returns:
        bool: True if all weapons are in types, False otherwise
    """
    # check if one weapon is not in types
    list_types = ["type_equipment", "weapon_type"]
    for col in list_types:
        if df[col].isnull().sum() > 0:
            print(f"At least one weapon is not in {col}")
            missing_weapons = df[df[col].isnull()]["weapon"].unique()
            print(f"Missing weapons in {col}:", missing_weapons)

            upd_data_artifact(
                f"At least one weapon is not in {col}",
                missing_weapons.tolist(),
            )
            create_artifact("dwh-subflow-ingestion-compo-weapons-artifact")
            return False

    return True


@task(
    name="Scrap page",
    task_run_name="task-scrap-page",
)
def scrap_page(page_content):
    """
    Extract data from page

    Args:
        page_content: page content

    Returns:
        df: DataFrame with data
    """
    # init list
    data_list = []

    # for each a component-link
    for component in page_content.find_all("a", class_="component-link"):

        # collect data
        yellow_elements = component.find_all("div", class_="yellow")

        # append to list
        data_list.append(
            {
                "product": yellow_elements[0].text,
                "refs": component.find(
                    "div", class_="text-white font-weight-bold"
                ).text,
                "weapon": yellow_elements[1].text,
                "manufacturer_country": yellow_elements[2].text,
                "manufacturer": yellow_elements[3].text,
            }
        )

    # create df
    return pd.DataFrame(data_list)


@flow(
    name="DWH Subflow Components in Weapons",
    flow_run_name="dwh-subflow-components-weapons",
    log_prints=True,
)
def flow_ingest_compo_weapons():
    """
    Retrieve data Components in Weapons from website
    """
    # vars
    today = datetime.datetime.now().strftime("%d.%m.%Y")
    MAX_PAGES = 1000  # for infinite loop
    count_new_data = 0
    all_scraped_data = []

    # read source
    df = read_data(PATH_DWH_SOURCES, "components_weapons")

    # define interval date
    if df.empty:
        start_date = "01.01.2024"
    else:
        start_date = df["ingest_date"].max().strftime("%d.%m.%Y")

    end_date = today
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # Url template
    url_base = f"https://war-sanctions.gur.gov.ua/en/components?f%5Bsearch%5D=&f%5Bcountry_id%5D=&f%5Bmanufacturer_id%5D=&f%5Btitle_uk%5D=&f%5Bpd%5D={start_date}+-+{end_date}&i%5Bmarking%5D=&page="

    for page_num in range(1, MAX_PAGES + 1):
        print(f"Processing page {page_num}")

        # Get the page
        response = requests.get(url_base + str(page_num))
        if response.status_code != 200:
            print(
                f"Erreur lors de la récupération de la page {page_num}: {response.status_code}"
            )
            break

        page_content = BeautifulSoup(response.content, "html.parser")

        # Scrap page
        df_tmp = scrap_page(page_content)

        # check if df_tmp is empty
        if df_tmp.empty:
            print("Empty page")
            break

        # count new data
        count_new_data += df_tmp.shape[0]

        # add ingest date
        df_tmp["ingest_date"] = today

        # append to list
        all_scraped_data.append(df_tmp)

        # check if last page
        if not get_pagination(page_content):
            print("Last page")
            break

    if all_scraped_data:
        # concat all data
        new_df = pd.concat(all_scraped_data, ignore_index=True)

        # convert date
        new_df["ingest_date"] = pd.to_datetime(new_df["ingest_date"], format="%d.%m.%Y")

        # concat with previous data
        df = pd.concat([df, new_df], ignore_index=True)
        df.drop_duplicates()

    # Links to Types
    df = add_types_weapons_equipment(df)

    # Check if weapons are in types
    if not check_weapons_in_types(df):
        return Failed(message="At least one weapon is not in types")

    # Artifact
    gestion_artifact(start_date, end_date, count_new_data)

    # save data
    save_data(PATH_DWH_SOURCES, "components_weapons", df)

    return Completed(message="Ingestion components weapons Completed")
