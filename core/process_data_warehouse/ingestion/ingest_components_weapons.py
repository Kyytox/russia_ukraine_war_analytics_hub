import os
import time
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd

from prefect import flow, task
from prefect.states import Completed

# Variables
from core.config.paths import PATH_DWH_BRONZE_COMPO_WEAPONS

# Functions
from core.libs.utils import concat_old_new_df

from core.common.parquet import save_parquet, read_parquet


@task(
    name="Scrap page",
    task_run_name="task-scrap-page",
)
def scrap_page(page_content, last_id=0):
    """
    Extract data from a single page content.

    Args:
        page_content: BeautifulSoup page content
        last_id: last id from previous data

    Returns:
        df: DataFrame with extracted data
        first_id: first id found on the page (or None if no components)
        stop: boolean indicating if we should stop (first_id <= last_id)
    """
    data_list = []
    first_id = None

    components = page_content.find_all("a", class_="component-link")

    if components:
        # Get first id on page
        first_component = components[0]
        first_id = int(first_component.get("href").split("/")[-1])

        # Check if first id is <= last_id -> stop
        if first_id <= last_id:
            print(f"First id on page {first_id} <= last_id {last_id}, stop run")
            return pd.DataFrame(data_list), first_id, True

    for component in components:
        yellow_elements = component.find_all("div", class_="yellow")
        id = int(component.get("href").split("/")[-1])

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

    return pd.DataFrame(data_list), first_id, False


@task(
    name="Get last id",
    task_run_name="get-last-id",
)
def get_last_id(df):
    """
    Get last id from existing DataFrame.

    Args:
        df: DataFrame

    Returns:
        last_id: int
    """
    if df.empty:
        return 0
    else:
        return df["id"].max()


def load_html_file(filepath):
    """
    Load and parse a single HTML file.

    Args:
        filepath: path to HTML file

    Returns:
        BeautifulSoup object or None if error
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return BeautifulSoup(f.read(), "html.parser")
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None


def get_html_files_sorted(path_html_files):
    """
    Get all HTML files from directory, sorted by page number.

    Args:
        path_html_files: directory path

    Returns:
        list of filepaths sorted by page number
    """
    if not os.path.exists(path_html_files):
        return []

    html_files = [
        f
        for f in os.listdir(path_html_files)
        if f.endswith(".html") and f.startswith("components_weapons_page_")
    ]

    # Sort by page number extracted from filename
    def extract_page_num(filename):
        try:
            # filename format: components_weapons_page_{num}_{date}.html
            return int(filename.split("_")[3])
        except (IndexError, ValueError):
            return 0

    return sorted(
        [os.path.join(path_html_files, f) for f in html_files], key=extract_page_num
    )


@flow(
    name="DWH Subflow Ingestion Components Weapons",
    flow_run_name="dwh-subflow-ingestion-components-weapons",
    log_prints=True,
)
def subflow_ingest_components_weapons():
    """
    Subflow Ingest Components Weapons

    - Get last id from existing data
    - Either fetch from URLs or process saved HTML files (controlled by use_saved_html)
    - Stop when first id on page <= last_id
    - Save data to bronze
    """

    # ===== CONFIGURATION =====
    use_saved_html = True  # Set to True to process saved HTML files instead of URLs

    # variables
    today = datetime.datetime.now().strftime("%d-%m-%Y")
    MAX_PAGES = 750
    path_html_files = f"{PATH_DWH_BRONZE_COMPO_WEAPONS}/html"

    # Create html directory if not exists
    os.makedirs(path_html_files, exist_ok=True)

    df_new = pd.DataFrame()

    # read source
    file_name = "components_weapons"
    print(f"Reading data from {PATH_DWH_BRONZE_COMPO_WEAPONS}")
    df = read_parquet(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name)

    # get last id
    last_id = get_last_id(df)
    print(f"Last id from existing data: {last_id}")

    # ===== MODE 1: PROCESS SAVED HTML FILES =====
    if use_saved_html:
        print("Mode: Processing saved HTML files")

        # Get all saved HTML files, sorted by page number
        html_files = get_html_files_sorted(path_html_files)

        if not html_files:
            print(f"No HTML files found in {path_html_files}")
            return Completed(message="No HTML files to process")

        print(f"Found {len(html_files)} HTML files to process")

        stop_run = False

        for filepath in html_files:
            print(f"Processing file: {filepath}")

            # Load HTML content
            page_content = load_html_file(filepath)
            if page_content is None:
                continue

            # Scrap page - returns df, first_id, stop
            df_tmp, first_id, stop_run = scrap_page(page_content, last_id)

            if stop_run:
                print(f"Stopping: first id {first_id} <= last_id {last_id}")
                break

            # Add data if not empty
            if not df_tmp.empty:
                df_new = pd.concat([df_new, df_tmp], ignore_index=True)
                print(
                    f"Added {len(df_tmp)} components from {os.path.basename(filepath)}"
                )

    # ===== MODE 2: FETCH FROM URLs =====
    else:
        print("Mode: Fetching from URLs")

        page_num = 1
        stop_run = False

        while page_num <= MAX_PAGES and not stop_run:
            url = f"https://war-sanctions.gur.gov.ua/en/components?page={page_num}&per-page=12"
            print(f"Processing page {page_num}: {url}")

            try:
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    print(f"Error getting page {page_num}: {response.status_code}")
                    break

                page_content = BeautifulSoup(response.content, "html.parser")

                # Save HTML page
                html_filename = (
                    f"{path_html_files}/components_weapons_page_{page_num}_{today}.html"
                )
                with open(html_filename, "w", encoding="utf-8") as file:
                    file.write(str(page_content))
                print(f"Saved HTML: {html_filename}")

                # Scrap page - returns df, first_id, stop
                df_tmp, first_id, stop_run = scrap_page(page_content, last_id)

                if stop_run:
                    print(f"Stopping: first id {first_id} <= last_id {last_id}")
                    break

                # Add data if not empty
                if not df_tmp.empty:
                    df_new = pd.concat([df_new, df_tmp], ignore_index=True)
                    print(f"Added {len(df_tmp)} new components from page {page_num}")

                page_num += 1
                time.sleep(1)

            except Exception as e:
                print(f"Error processing page {page_num}: {e}")
                break

    # ===== COMMON: SAVE RESULTS =====
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

    print(df.head(50))

    # save data
    save_parquet(PATH_DWH_BRONZE_COMPO_WEAPONS, file_name, df)

    return
