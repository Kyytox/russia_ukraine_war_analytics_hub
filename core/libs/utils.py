import os
import re
import json
import pandas as pd
from prefect import task
from prefect.runtime import task_run
from prefect.variables import Variable
from prefect.artifacts import create_table_artifact


# Variables
from core.config.paths import PATH_JSON_RU_REGION
from core.config.variables import LIST_ACCOUNTS_TELEGRAM


def upd_data_artifact(info, data):
    """
    Update data artifact

    Args:
        info: information
        data: data to add
    """

    # get varoaible
    data_artifact = Variable.get("data_artifact", default=[])

    # add data
    new_data = {
        "info": info,
        "data": data,
    }
    data_artifact.append(new_data)

    # save variable
    Variable.set("data_artifact", data_artifact, overwrite=True)


def create_artifact(key_name):
    """
    Create artifact

    Args:
        key: key of artifact
    """

    # get data
    data_artifact = Variable.get("data_artifact", default=[])

    # create data
    create_table_artifact(
        key=key_name,
        table=data_artifact,
    )

    # reset variable
    Variable.set("data_artifact", [], overwrite=True)


@task(name="Get telegram accounts", task_run_name="get-telegram-accounts")
def get_telegram_accounts(path: str) -> list:
    """
    Get telegram accounts

    Args:
        path: path to accounts

    Returns:
        List of accounts
    """
    # get accounts
    list_accounts = [
        file_name.split(".")[0]
        for file_name in os.listdir(path)
        if file_name.endswith(".parquet")
    ]

    if not list_accounts:
        list_accounts = LIST_ACCOUNTS_TELEGRAM

    print(f"Accounts: {list_accounts}")

    return list_accounts


def generate_task_name():
    """
    Generate task name

    Returns:
        Task name
    """
    # get task name
    task_name = task_run.get_name().split(" ")[0]

    # get parameters
    params = task_run.get_parameters()

    # get file name
    file = f"{params['base_path'].split('/')[-1]}/{params['file_name']}"
    return f"{task_name.lower()}-{file}"


@task(
    name="Read data",
    task_run_name=generate_task_name,
    tags=["read"],
)
def read_data(base_path: str, file_name: str) -> pd.DataFrame:
    """
    Read data from parquet

    Args:
        base_path: base path
        file_name: file name

    Returns:
        Dataframe with parquet data
    """
    # get data
    try:
        df = pd.read_parquet(f"{base_path}/{file_name}.parquet", engine="fastparquet")
    except Exception as e:
        print(e)
        df = pd.DataFrame()

    print(f"Reading {df.shape} data from {base_path}/{file_name}.parquet")
    return df


@task(name="Save data", task_run_name=generate_task_name, tags=["save"])
def save_data(
    base_path: str, file_name: str, df: pd.DataFrame, partition_cols: list | None = None
):
    """
    Save data to parquet

    Args:
        base_path: base path
        file_name: file name
        df_old: old dataframe
        df: dataframe to save
        partition_cols: list of columns to partition

    Returns:
        None
    """

    if df.empty:
        print(f"No data to save")
        return

    # create folder if not exist
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    path = os.path.join(base_path, f"{file_name}.parquet")
    print(f"Saving {df.shape} data to {path}")

    # remove dir
    # os.remove(path)

    # save data to parquet
    df.to_parquet(
        path,
        engine="fastparquet",
        partition_cols=partition_cols,
        compression="snappy",
    )

    return


@task(name="Filter data to process", task_run_name="filter-data-to-process")
def keep_data_to_process(
    df_source: pd.DataFrame, df_to_filter: pd.DataFrame
) -> pd.DataFrame:
    """
    Filter data

    Args:
        df_source: dataframe
        df_to_filter: dataframe to filter

    Returns:
        Dataframe with filtered data
    """

    # keep data not in clean data
    if df_to_filter.empty:
        df = df_source
    else:
        df = df_source[~df_source["ID"].isin(df_to_filter["ID"])].reset_index(drop=True)

    return df


@task(name="Concat old and new data", task_run_name="concat-old-new-data")
def concat_old_new_df(
    df_raw: pd.DataFrame, df_new: pd.DataFrame, cols: list
) -> pd.DataFrame:
    """
    Concatenate old and new dataframes
    Drop duplicates by cols or not
    Sort by date

    Args:
        df_raw: old dataframe
        df_new: new dataframe
        cols: list of columns to drop duplicates

    Returns:
        df: dataframe
    """

    df = (
        pd.concat([df_raw, df_new])
        .drop_duplicates(subset=cols if len(cols) > 0 else None, keep="last")
        .sort_values("date" if "date" in df_new.columns else [])
        .reset_index(drop=True)
    )
    return df


@task(name="Get region Géojson", task_run_name="get-region-geojson")
def get_regions_geojson():
    """
    Get the region and id from the json file
    """

    # read file json
    with open(PATH_JSON_RU_REGION) as file:
        data = json.load(file)

    # get id and name
    dict_region = {}
    for i in range(len(data["features"])):
        dict_region[data["features"][i]["properties"]["name"]] = data["features"][i][
            "id"
        ]

    # update dict
    dict_region = {
        k.replace("Moskva", "Moscow")
        .replace("'", "")
        .replace("Moscow Oblast", "Moscow")
        .replace("Moscow City", "Moscow"): v
        for k, v in dict_region.items()
    }

    return dict_region


def format_clean_text(text):
    """
    Formate and clean text

    Args:
        text: text

    Returns:
        Text cleaned
    """
    # Remove special characters
    regex_pattern = re.compile(
        pattern="["
        "\U0001f600-\U0001f64f"  # emoticons
        "\U0001f300-\U0001f5ff"  # symbols & pictographs
        "\U0001f680-\U0001f6ff"  # transport & map symbols
        "\U0001f1e0-\U0001f1ff"  # flags (iOS)
        "\U00002500-\U00002bef"  # chinese char
        "\U00002702-\U000027b0"
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2b55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+",
        flags=re.UNICODE,
    )
    text = regex_pattern.sub("", text)

    # Remove unwanted text
    motifs = [
        r"Подписывайся на SHOT",
        r"Прислать новость (https://t.me/shot_go)",
        r"Прислать новость",
        r"Предложить свою новость",
        r"Подслушано электрички Москвы",
        r"#Новости@transport_online",
        r"Минтранс Подмосковья",
        r"Прислать фото/видео/информацию: @Astra4bot",
        r"Резервный канал ASTRA: https://t.me/astrapress2",
        r"astrapress@protonmail.com",
        r"Предложить свою новость (https://t.me/electrichkibot)",
        r"Предложить свою новость",
        r"Подслушано электрички Москвы (https://t.me/electrichki)",
        r"Подслушано электрички Москвы",
        r"SHOT",
    ]
    for motif in motifs:
        text = re.sub(motif, "", text)

    # remove after
    text = re.sub(r"\s*(Фото|Видео)\s*\S*\s*от:.*?\n", "", text)

    # remove http and @
    text = re.sub(r"@\S+|http\S+", "", text)
    text = re.sub(r"Subscribe to SHOT.*", "", text)
    text = re.sub(r"@Astra4botРезервный.*|Прислать фото/видео/информацию:", "", text)
    text = re.sub(r"\s*—\s*", ". ", text)
    text = re.sub(r"—\s*", "", text)
    text = re.sub(r"&amp;", "", text)

    # Remove first and last character if
    if text.startswith(" "):
        text = text[1:]
    if text.endswith(":"):
        text = text[:-1]

    # Remove unwanted text translations
    translations = [
        "Here is the translation:",
        "Here is the English translation:",
        "Here is the translation to English:",
        "Here's the translation:",
        "Translation in English:",
        "I'm ready to translate.",
        "The translation is:",
        "Translation:",
        "I'll translate the text for you:",
        "I'll translate the text accurately,",
        "Translation in English as listed:",
        "I can translate this text for you.",
        "I'll translate the text accurately from Russian to English while maintaining the context and tone of the original.",
        "I'll translate the text accurately and grammatically correct.",
        "Here is the translation of your text from Russian to English:",
    ]
    for trad in translations:
        text = text.replace(trad, "")

    if not text:
        return None

    text = re.sub(r" +", " ", text).strip()

    return text


@task(name="Rename columns", task_run_name="rename-columns")
def rename_cols(df, dict_cols):
    """
    Update the columns names

    Args:
        df (pd.DataFrame): Dataframe to control
        dict_cols (dict): Dictionary with old and new names

    Returns:
        pd.DataFrame: Dataframe with new columns names
    """

    # format names
    df.columns = df.columns.str.strip()

    # rename columns
    return df.rename(columns=dict_cols)


@task(name="Retype columns", task_run_name="retype-columns")
def retype_cols(df, dict_types):
    """
    Retype columns

    Args:
        df (pd.DataFrame): Dataframe to control
        dict_types (dict): Dictionary with columns and types

    Returns:
        pd.DataFrame: Dataframe with retyped columns
    """

    for col, dtype in dict_types.items():
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                df[col] = df[col].astype(dtype)

    return df


@task(name="Drop Duplicates", task_run_name="drop-duplicates")
def drop_duplicates(df, list_cols):
    """
    Drop duplicates by col

    Args:
        df (pd.DataFrame): Dataframe to control
        list_cols (list): List of columns to control

    Returns:
        df (pd.DataFrame): Dataframe without duplicates
    """

    for col in list_cols:
        df = df.drop_duplicates(subset=[col])

    df = df.drop_duplicates().reset_index(drop=True)

    return df
