import os
import datetime
from duckdb import read_parquet
import pandas as pd

import requests
from bs4 import BeautifulSoup

from prefect import flow, task
from prefect.states import Completed

# Variables
from core.config.paths import PATH_DWH_BRONZE_COMPO_WEAPONS

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    concat_old_new_df,
)

from core.common.parquet import save_parquet, read_parquet
from core.common.dataframe import reorder_cols, retype_cols


@task(
    name="Scrap page",
    task_run_name="task-scrap-page",
)
def scrap_page(page_content, last_id=0):
    """
    Extract data from page

    Args:
        page_content: page content
        last_id: last id find in previous data

    Returns:
        df: DataFrame with data

    """
    # init list
    data_list = []

    # for each a component-link
    for component in page_content.find_all("a", class_="component-link"):
        # collect data
        yellow_elements = component.find_all("div", class_="yellow")
        id = int(component.get("href").split("/")[-1])

        # Check if id already exists in previous data
        if id <= last_id:
            print(f"Id {id} <= last_id {last_id}, stop run")
            return pd.DataFrame(data_list), True

        # append to list
        data_list.append(
            {
                "id": id,
                "product": yellow_elements[0].text,
                "refs": component.find(
                    "div", class_="text-white font-weight-bold"
                ).text,
                "weapon_details": yellow_elements[1].text,
                "manufacturer_country": yellow_elements[2].text,
                "manufacturer": yellow_elements[3].text,
            }
        )

    # create df
    return pd.DataFrame(data_list), False


@task(
    name="Get last id",
    task_run_name="get-last-id",
)
def get_last_id(df):
    """
    Get last id components weapons

    Args:
        df: DataFrame

    Returns:
        last_id: int
    """
    if df.empty:
        return 0
    else:
        return df["id"].max()


@flow(
    name="DWH Subflow Ingestion Components Weapons",
    flow_run_name="dwh-subflow-ingestion-components-weapons",
    log_prints=True,
)
def subflow_ingest_components_weapons():
    """
    Subflow Ingest Components Weapons

    - Scrap data from website
    - Save data to bronze
    """

    # variables
    today = datetime.datetime.now().strftime("%d-%m-%Y")
    MAX_PAGES = 1000  # for infinite loop
    path_html_files = f"{PATH_DWH_BRONZE_COMPO_WEAPONS}/html"
    df_new = pd.DataFrame()

    # read source
    file_name = "components_weapons"
    # df = read_data(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name)
    print(f"Reading data from {PATH_DWH_BRONZE_COMPO_WEAPONS}")
    df = read_parquet(PATH_DWH_BRONZE_COMPO_WEAPONS, "components_weapons")

    # get last id
    last_id = get_last_id(df)
    print(f"Last id: {last_id}")

    #
    #
    # FOR TESTS
    # Get all files html
    list_html_files = os.listdir(path_html_files)
    list_html_files = [
        # file for file in list_html_files if file.startswith("components_weapons_page_")
        file
        for file in list_html_files
        if file.endswith(".html")
    ]

    for file in list_html_files:
        with open(os.path.join(path_html_files, file), "r") as f:
            page_content = BeautifulSoup(f.read(), "html.parser")
            print(f"Processing file {file}")
        #
        #
        #

        # for page_num in range(1, MAX_PAGES + 1):
        #     url = f"https://war-sanctions.gur.gov.ua/en/components?page={page_num}&per-page=12"
        #     print(f"Processing page {page_num}")
        #
        #     # Get the page
        #     response = requests.get(url)
        #     if response.status_code != 200:
        #         print(f"Err Get page {page_num}: {response.status_code}")
        #         break
        #
        #     page_content = BeautifulSoup(response.content, "html.parser")

        # save html page
        # with open(f"{path_html_files}/page_{page_num}_{today}.html", "w") as file:
        #     file.write(str(page_content))

        # Scrap page
        df_tmp, stop_run = scrap_page(page_content, last_id)
        df_new = pd.concat([df_new, df_tmp], ignore_index=True)

        if stop_run:
            break

    if df_new.empty:
        print("No new data")
        return Completed(message="Ingestion components weapons Completed")

    # add ingest date
    df_new["ingest_date"] = today

    # concat with previous data
    df = (
        concat_old_new_df(df, df_new, ["id"])
        .sort_values(by="id", ascending=False)
        .reset_index(drop=True)
    )

    # save data
    # save_data(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name, df)
    save_parquet(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name, df)

    return
