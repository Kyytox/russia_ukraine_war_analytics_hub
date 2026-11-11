import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from prefect import flow, task
from prefect.states import Completed, Failed

# Variables
from core.config.paths import PATH_DWH_SOURCES, PATH_DWH_SOURCES_RU_OFFICERS_KIU
from core.config.variables import ID_EXCEL_RU_OFFICERS_KIU
from core.config.dwh_corresp_schema import CORRESP_SCHEMA_RUSSIA_BLOCK_SITES

# Functions
from core.libs.utils import (
    save_data,
    create_artifact,
    upd_data_artifact,
    rename_cols,
    drop_duplicates,
)
from core.libs.google_api import (
    connect_google_sheet_api,
    get_sheet_data,
)


from core.config.variables import GROUP_SIZE_MILITARY_UNIT

# IA
from core.libs.ollama_ia import ia_treat_message


@task(name="Format Columns Names", task_run_name="format-cols-names")
def format_cols_names(df):
    """
    Format the columns names

    Args:
        df (pd.DataFrame): Dataframe to update

    Returns:
        df (pd.DataFrame): Dataframe with formatted columns names
    """

    # format cols names, split \n
    df.columns = (
        df.columns.str.split("\n")
        .str[0]
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
    )

    return df


@task(name="Format values", task_run_name="format-values")
def format_values(df):
    """
    Format the values in the columns

    Args:
        df (pd.DataFrame): Dataframe to control

    Returns:
        df (pd.DataFrame): Dataframe with formatted values
    """
    return df


@task(
    name="Translate and extract Military Unit Position",
    task_run_name="translate-and-extract-military-unit-position",
)
def process_military_unit(df):
    """
    Translate and extract military unit position from the dataframe

    Args:
        df: dataframe to process

    Returns:
        Dataframe with translated military unit position and extracted military unit
    """

    df_result_trans = pd.DataFrame()
    df_result_unit = pd.DataFrame()
    path_file = f"{PATH_DWH_SOURCES_RU_OFFICERS_KIU}/military_unit_translate.csv"

    # replace NAN by None
    df = df.replace({np.nan: None})

    # get unique military unit positions
    df_milit = (
        df[["military_unit_position"]].drop_duplicates().reset_index(drop=True)
    ).dropna()

    # get file military unit already translated if exist
    if os.path.exists(path_file):
        df_milit_trans = pd.read_csv(path_file)

        # merge with df_milit
        df_milit = df_milit.merge(
            df_milit_trans, on="military_unit_position", how="left"
        )
    else:
        df_milit["military_unit_position_translate"] = None
        df_milit["military_unit"] = None

    prompt = """Translate the text from Russian/Ukrainian to English. If the text is not in Russian/Ukrainian, return the text as is. Return only the translated text, without any other information.

    if the text contains one of their abbreviations, replace it with the full name in English:

    - мсп -> Motor Rifle Regiment
    - омсбр -> Separate Guards Motor Rifle Brigade
    - БАРС -> BARS
    - оибр -> Guards Spetsnaz Brigade
    - в/ч -> Military Unit
    - Ахмат -> Akhmat
    - ВДВ -> Airborne Forces
    - тп -> Tank Regiment
    - абр -> Artillery Brigade
    - пдп -> Paratrooper Regiment
    - обрмп -> Guards Naval Infantry Brigade
    - прхбз -> Guards Radiological, Chemical and Biological (RCB) Protection

    """

    # get data to translate
    df_to_trans = (
        df_milit[df_milit["military_unit_position_translate"].isna()][
            ["military_unit_position"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    print("Military unit to translate", df_to_trans.shape)

    # Calcul number of batches
    total_batches = (
        len(df_to_trans) + GROUP_SIZE_MILITARY_UNIT - 1
    ) // GROUP_SIZE_MILITARY_UNIT

    with tqdm(total=total_batches, desc="Translate data") as pbar:
        for i in range(0, len(df_to_trans), GROUP_SIZE_MILITARY_UNIT):
            batch = df_to_trans.iloc[i : i + GROUP_SIZE_MILITARY_UNIT].copy()

            # ask IA
            batch["military_unit_position_translate"] = batch[
                "military_unit_position"
                # ].apply(lambda x: ia_treat_message(x, "ru_officers_kiu_translate"))
            ].apply(
                lambda x: ia_treat_message(x, "ru_officers_kiu_military_unit", prompt)
            )
            df_result_trans = pd.concat([df_result_trans, batch], ignore_index=True)

            # Update progress bar
            pbar.update(1)

    # Add col date_last_update with current date
    df_result_trans["date_last_update"] = pd.Timestamp.now()

    # concat result trans with df_milit
    df_milit = pd.concat(
        [df_milit, df_result_trans], ignore_index=True
    ).drop_duplicates(subset=["military_unit_position"], keep="last")

    # # replace values abreviations
    # df_milit["military_unit_position_translate"] = df_milit[
    #     "military_unit_position_translate"
    # ].replace(
    #     {
    #         "Спецназ Росгвардия": "Rosgvardia Special Forces",
    #         "ОМОН, МВД": "Riot Police, Ministry of Internal Affairs",
    #         "Внутренние войска МВД": "Internal Troops of the Ministry of Internal Affairs",
    #         "ОМОН/СОБР": "Riot Police/Special Forces",
    #         "ГИБДД МВД": "Traffic Police, Ministry of Internal Affairs",
    #         "ГО и ЧС": "Civil Defense and Emergency Situations",
    #         "«Гром» МВД": "Thunder, Ministry of Internal Affairs",
    #         "МВД - Major": "MVD - Major",
    #         "Росгвардия": "Russian Guard",
    #         "УФСИН": "Federal Penitentiary Service",
    #         "Центр специального назначения «Витязь»": "Vityaz Special Purpose Center",
    #         "ГИБДД": "Traffic Police",
    #         "ФСИН": "Federal Penitentiary Service",
    #         "ФТС": "Federal Customs Service",
    #         "ФССП": "Federal Bailiff Service",
    #         "РОВД": "District Department Police",
    #         "ФСО": "Federal Protective Service",
    #         "СК": "Investigative Committee",
    #         "MVD": "Ministry of Internal Affairs",
    #         "СОБР": "Special Rapid Response Unit",
    #         "МВД": "Ministry of Internal Affairs",
    #         "ФКСН": "Federal Drug Control Service",
    #         "МЧС": "Ministry of Emergency Situations",
    #         "ОМОН": "Riot Police",
    #         "nan": np.nan,
    #     }
    # )
    print("Military unit after merge", df_milit.shape)
    print(df_milit.head(50))

    # ask IA to extract military unit from military_unit_position_translate
    prompt = """Extract the military unit from the text. If there is no military unit,
    return None. Return only the military unit name, without any other information.

    Examples:
    Input: "Commander of the 1st Battalion, 2nd Brigade, 3rd Division"
    Output: "1st Battalion, 2nd Brigade, 3rd Division"

    Input: "Commander of the 33rd Motor Rifle Regiment"
    Output: "33rd Motor Rifle Regiment"

    Input: "Company from 37th Brigade Combat Group."
    Output: "37th Brigade Combat Group"

    Input: "v/ч 29660"
    Output: "Unit 29660"

    Input: "Headquarters of the volunteer unit "BARS-13"
    Output: "BARS-13"

    Don't forget to add st, nd, rd, th after the number if it is not present in the text.

    If u don't find exactly the military unit, return None. Don't try to guess the military unit. If you don't find it, return None.
    """

    # Get unique military unit to extract
    df_milit_unit = (
        df_milit[df_milit["military_unit"].isna()][
            ["military_unit_position", "military_unit_position_translate"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    print("Military unit to extract", df_milit_unit.shape)

    # Calcul number of batches
    total_batches = (
        len(df_milit_unit) + GROUP_SIZE_MILITARY_UNIT - 1
    ) // GROUP_SIZE_MILITARY_UNIT

    with tqdm(total=total_batches, desc="Extract military unit") as pbar:
        for i in range(0, len(df_milit_unit), GROUP_SIZE_MILITARY_UNIT):
            batch = df_milit_unit.iloc[i : i + GROUP_SIZE_MILITARY_UNIT].copy()

            # ask IA
            batch["military_unit"] = batch["military_unit_position_translate"].apply(
                lambda x: ia_treat_message(x, "ru_officers_kiu_military_unit", prompt)
            )
            df_result_unit = pd.concat([df_result_unit, batch], ignore_index=True)

            # Update progress bar
            pbar.update(1)

    # Add col date_last_update with current date
    df_result_unit["date_last_update"] = pd.Timestamp.now()

    # Replace None values with NaN
    df_result_unit["military_unit"] = df_result_unit["military_unit"].replace(
        {"None": np.nan}
    )

    # concat result trans with df_milit
    df_milit = pd.concat([df_milit, df_result_unit], ignore_index=True).drop_duplicates(
        subset=["military_unit_position"], keep="last"
    )
    print("Military unit after merge", df_milit.shape)
    print(df_milit.head(50))

    # merge with df
    df = df.merge(df_milit, on="military_unit_position", how="left")

    # save translation military unit for the next passage
    # like that we don't have to translate again the same military unit
    df_milit.to_csv(path_file, index=False)

    return df


@task(name="Process Sheet Ranks", task_run_name="process-sheet-ranks")
def process_sheet_ranks(df):
    """
    Process the sheet Ranks

    Args:
        df (pd.DataFrame): Dataframe to process

    Returns:
        df (pd.DataFrame): Processed dataframe
    """

    # remove first line becasue is the number of elements (useless)
    df = df.iloc[1:, :]

    # format cols names
    df = format_cols_names(df)

    # Pivot table, put columns in cols name 'ranks' and values in col 'name_officer'
    df = (
        df.melt(var_name="rank", value_name="name_officer")
        .dropna(subset=["name_officer"])
        .reset_index(drop=True)
    )
    # fromart values
    df["name_officer"] = df["name_officer"].str.strip()

    return df


@task(name="Format Date KIA", task_run_name="format-date-kia")
def format_date_kia(df, col_date, new_col):
    """
    Format the date_kia column

    Args:
        df (pd.DataFrame): Dataframe to process
        col_date (str): Column name to format
        new_col (str): New column name for formatted date

    Returns:
        df (pd.DataFrame): Dataframe with formatted date_kia column
    """
    print("Unique values in", col_date)

    # Empty to NaN
    df[col_date] = df[col_date].replace({"": np.nan})

    # replace words
    df[col_date] = (
        df[col_date]
        .str.strip()
        .str.rstrip()
        .replace(
            {
                "\(": "",
                "\)": "",
                "Jan ": "January ",
                "died ": "",
                "fall": "October",
                "Feb or March": "March",
                "MIA April": "April",
                "MIA December 19,": "December",
                "MIA December 17,": "December",
                "March-June": "March",
                "early June": "June",
                ", last time active on VK on 16 December": "",
                "probably May": "May",
                "probably sep": "September",
                "Beginning of July,": "July",
                "Mid January": "January",
                "after January": "January",
                ".October": " October",
                ".September": " September",
                "6 September 2023": "6. September 2023",
                "20.October": "20. October",
                "6.September": "6. September",
                "July,": " July",
                "end November": "November",
                "Dec ": "December ",
                "before 2025": "2024",
                "MIA 2023": "2023",
                "\?\. ": "",
                "\?": "",
                # "in ": "",
            },
            regex=True,
        )
    )

    def add_01_to_date(value):
        if isinstance(value, str) and value[0].isupper():
            return "1. " + value
        return value

    # if date_kia start with caractere in UPPER, add '01' before (don't do if it's number)
    df[col_date] = df[col_date].apply(add_01_to_date)

    # convert to date
    df[new_col] = pd.to_datetime(df[col_date], errors="coerce").dt.date

    print(
        "date_kia without '.' and notnull",
        df[
            df[col_date].notna()  # On garde uniquement les valeurs non-NaN
            & ~df[col_date].str.contains(".", regex=False, na=False)
        ][col_date].unique(),
    )

    print(
        df[
            df[new_col].isna()
            & df[col_date].notna()
            & ~df[col_date].str.contains("in", regex=False, na=False)
        ].head(50)
    )

    return df


@task(name="Process Sheet Sources", task_run_name="process-sheet-sources")
def process_sheet_sources(df):
    """
    Process the sheet Sources

    Args:
        df (pd.DataFrame): Dataframe to process

    Returns:
        df (pd.DataFrame): Processed dataframe
    """
    # Format cols names
    df = format_cols_names(df)
    df = df.rename(columns={"name": "name_officer"})

    # Format values
    df["rank"] = (
        df["rank"].str.strip().str.lower().str.replace(" ", "_").str.replace("/", "_")
    )
    df["memorial_plaque"] = df["memorial_plaque"].replace({"1": True, np.nan: False})
    df["name_officer"] = df["name_officer"].str.strip()

    # Rename columns
    df = df.rename(columns={"date_kia": "date_kia_str"})
    df = df.rename(
        columns={"date_kia_according_to_@splussi_research": "date_kia_splussi_str"}
    )

    # Format date_kia
    df = format_date_kia(df, "date_kia_splussi_str", "date_kia_splussi")
    df = format_date_kia(df, "date_kia_str", "date_kia")

    # create date_kiu with date_kia_splussi else date_kia
    df["date_kiu"] = np.where(
        df["date_kia_splussi"].notna(), df["date_kia_splussi"], df["date_kia"]
    )

    # Year, Month, Day
    df["year_kiu"] = pd.DatetimeIndex(df["date_kiu"]).year
    df["month_kiu"] = pd.DatetimeIndex(df["date_kiu"]).month
    df["day_kiu"] = pd.DatetimeIndex(df["date_kiu"]).day

    # replace police values
    df["police"] = df["police"].replace(
        {
            "Спецназ Росгвардия": "Rosgvardia Special Forces",
            "ОМОН, МВД": "Riot Police, Ministry of Internal Affairs",
            "Внутренние войска МВД": "Internal Troops of the Ministry of Internal Affairs",
            "ОМОН/СОБР": "Riot Police/Special Forces",
            "ГИБДД МВД": "Traffic Police, Ministry of Internal Affairs",
            "ГО и ЧС": "Civil Defense and Emergency Situations",
            "«Гром» МВД": "Thunder, Ministry of Internal Affairs",
            "МВД - Major": "MVD - Major",
            "Росгвардия": "Russian Guard",
            "УФСИН": "Federal Penitentiary Service",
            "Центр специального назначения «Витязь»": "Vityaz Special Purpose Center",
            "ГИБДД": "Traffic Police",
            "ФСИН": "Federal Penitentiary Service",
            "ФТС": "Federal Customs Service",
            "ФССП": "Federal Bailiff Service",
            "РОВД": "District Department Police",
            "ФСО": "Federal Protective Service",
            "СК": "Investigative Committee",
            "MVD": "Ministry of Internal Affairs",
            "СОБР": "Special Rapid Response Unit",
            "МВД": "Ministry of Internal Affairs",
            "ФКСН": "Federal Drug Control Service",
            "МЧС": "Ministry of Emergency Situations",
            "ОМОН": "Riot Police",
            "nan": np.nan,
        }
    )

    # Replace PMC values
    df["pmc"] = df["pmc"].replace(
        {
            "ЧВК «Вагнер»": "Wagner Group",
            "Вагнер": "Wagner Group",
            "Wagner": "Wagner Group",
            "Gazprom POTOK": "Gazprom PMC",
            "Gazprom TORCH": "Gazprom PMC",
        }
    )

    # Replace intel values
    df["intel"] = df["intel"].replace(
        {
            "ФСБ": "FSB",
            "Спецназ ГРУ": "GRU Spetsnaz",
            "ГРУ": "GRU",
            "СОБР": "SOBR",
            "Спецназ ВВ МВД": "MVD Internal Troops Spetsnaz",
            "Следком": "Investigative Committee",
            "ФСБ (Spetsnaz)": "FSB (Spetsnaz)",
        }
    )

    # Translate military_unit_position
    # df = df.head(500)
    df = process_military_unit(df)

    exit()

    return df


@flow(
    name="DWH Subflow Ingestion Russian Officers KIU",
    flow_run_name="dwh-subflow-ingestion-ru-officers-kiu",
    log_prints=True,
)
def flow_ingest_ru_officers_kiu():
    """
    Integration Russian Officers KIU
    """

    # connect to google sheet
    # service = connect_google_sheet_api()
    #
    # define sheet
    # spreadsheet_id = ID_EXCEL_RU_OFFICERS_KIU
    # range_ranks = "Ranks!A3:N"
    # range_sources = "Sources!A2:Q"
    #
    # # get data from sheet - ranks
    # df_ranks = get_sheet_data(service, spreadsheet_id, range_ranks)
    # df_sources = get_sheet_data(service, spreadsheet_id, range_sources)
    #
    # # Save for test
    # df_ranks.to_csv("df_ranks.csv", index=False)
    # df_sources.to_csv("df_sources.csv", index=False)

    # read csv
    df_ranks = pd.read_csv(f"{PATH_DWH_SOURCES_RU_OFFICERS_KIU}/df_ranks.csv")
    df_sources = pd.read_csv(f"{PATH_DWH_SOURCES_RU_OFFICERS_KIU}/df_sources.csv")

    print("\n Ranks \n", df_ranks)
    print("\n Ranks \n", df_ranks.dtypes)
    print("\n Sources \n", df_sources)
    print("\n Sources \n", df_sources.dtypes)
    # process ranks sheet
    df_ranks = process_sheet_ranks(df_ranks)
    print("\n Ranks \n", df_ranks)
    print("\n Ranks \n", df_ranks.dtypes)

    # process sources sheet
    df_sources = process_sheet_sources(df_sources)
    print("\n Sources \n", df_sources.head(50))
    print("\n Sources \n", df_sources[["date_kia_str", "memorial_plaque", "date_kia"]])
    print("\n Sources \n", df_sources.dtypes)

    # merge ranks and sources
    print(df_ranks.shape, df_sources.shape)

    # df = df_ranks.merge(
    #     df_sources,
    #     on=["rank", "name_officer"],
    #     how="left",
    # )
    #
    # print("\n Merged \n", df.shape)
    # print("\n Merged \n", df)
    # print("\n Merged \n", df.dtypes)

    # upd_data_artifact(f"Ingestion Russian Officers KIU", df.shape[0])

    # # save data
    # save_data(PATH_DWH_SOURCES, "ru_officers_kiu", df)

    # # create artifact
    # create_artifact("dwh-subflow-ingestion-ru-officers-kiu-artifact")

    # return Completed(message="Ingestion Russian Officers KIU completed")
