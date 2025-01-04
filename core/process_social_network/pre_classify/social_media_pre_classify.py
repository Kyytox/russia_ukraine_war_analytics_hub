import re
import pandas as pd
import numpy as np
from prefect import flow, task


# Functions
from core.libs.utils import (
    get_regions_geojson,
    concat_old_new_df,
    read_data,
    save_data,
)

from core.libs.ollama_ia import ia_treat_message

# Variables
from core.config.paths import (
    PATH_FILTER_SOCIAL_MEDIA,
    PATH_PRE_CLASSIFY_SOCIAL_MEDIA,
)
from core.config.variables import (
    DICT_LAWS,
)
from core.config.schemas import (
    SCHEMA_PRE_CLASS_RAILWAY,
    SCHEMA_PRE_CLASS_ARREST,
    SCHEMA_PRE_CLASS_SABOTAGE,
)


@task(name="Classify with IA")
def pre_classify_with_ia(df, col_filter, col_add_final):
    """
    Classify with IA

    Args:
        df: dataframe
        col_filter: column to filter
        col_add_final: column to add final

    Returns:
        Dataframe with pre classified data
    """
    print(f"Data to classify: {df.shape}")

    if "pre_class_ia" not in df.columns:
        df["pre_class_ia"] = ""

    # get data to classify with ia, only 300 for avoid errors
    df_pre_class_ia = (
        df[
            # (df[col_add_final] == False)
            # & (df[col_filter])
            # &
            (df["pre_class_ia"].isnull() | (df["pre_class_ia"] == ""))
        ]
        .head(3)
        .reset_index(drop=True)
    )

    print(f"Data to classify with IA: {df_pre_class_ia.shape}")

    for i, row in df_pre_class_ia.iterrows():
        print(f"Index: {i}")

        # get text
        if row["text_translate"] == "":
            text = row["text_original"]
        else:
            text = row["text_translate"]

        # ask IA
        df_pre_class_ia.loc[i, "pre_class_ia"] = ia_treat_message(text, "classify")

    # concat data
    df_concat = (
        (pd.concat([df, df_pre_class_ia], ignore_index=True))
        .drop_duplicates(subset=["ID"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )
    print(df_concat.shape)

    # replace Nan
    df_concat["pre_class_ia"] = df_concat["pre_class_ia"].fillna("")

    print(f"Data classified: {df_concat.shape}")

    return df_concat


def find_region(text, LIST_REGIONS):
    """
    Find region in text
    If multiple regions in text, return ""
    else return region

    Args:
        text: text
        LIST_REGIONS: list of regions

    Returns:
        Region
    """
    # find only one region
    regex = r"\b(?:" + "|".join(map(re.escape, LIST_REGIONS)) + r")\b"
    regions = re.findall(regex, text, re.IGNORECASE)

    # if only one region
    if len(regions) == 1:
        return regions[0]
    else:
        return ""


@task(name="Pre classify Region")
def pre_classify_region(df):
    """
    Pre classify Region

    Args:
        df: dataframe

    Returns:
        Dataframe with pre classified region
    """

    # get regions
    dict_regions = get_regions_geojson()

    # put keys in list
    LIST_REGIONS = list(dict_regions.keys())

    # add col region
    df["pre_class_region"] = ""

    # for text_translate not null and not empty
    mask = df["text_translate"].notna() & (df["text_translate"] != "")

    # find region
    df.loc[mask, "pre_class_region"] = df.loc[mask, "text_translate"].apply(
        lambda x: find_region(x, LIST_REGIONS)
    )

    return df


# find law
def find_law(text, DICT_LAWS):
    """
    Find law in text

    Args:
        text: text
        DICT_LAWS: dict with laws

    Returns:
        Laws found
    """
    found_laws = set()
    for law, terms in DICT_LAWS.items():
        for term in terms:
            if re.search(rf"\b{re.escape(term)}\b", text, re.IGNORECASE):
                found_laws.add(law)
    return ", ".join(found_laws) if found_laws else ""


@task(name="Pre classify Applied Laws")
def pre_classify_app_laws(df):
    """
    Pre classify Applied Laws

    Args:
        df: dataframe

    Returns:
        Dataframe with pre classified applied laws
    """
    # add col pre_class_app_laws
    df["pre_class_app_laws"] = ""

    # for text_translate not null and not empty
    mask = df["text_translate"].notna() & (df["text_translate"] != "")

    df.loc[mask, "pre_class_app_laws"] = df.loc[mask, "text_translate"].apply(
        lambda x: find_law(x, DICT_LAWS)
    )

    return df


@task(name="Remove cols filter")
def remove_cols_filter(df):
    """
    Remove cols filter
    - cols_origin
    - start with found_
    - start with filter_
    - start with add_final_

    Args:
        df: dataframe

    Returns:
        Dataframe without cols filter
    """

    cols_origin = ["text_original", "text_translate"]
    cols_terms = [col for col in df.columns if col.startswith("found_")]
    cols_filter = [col for col in df.columns if col.startswith("filter_")]
    cols_add_final = [col for col in df.columns if col.startswith("add_final_")]

    df = df.drop(columns=cols_origin + cols_terms + cols_filter + cols_add_final)

    return df


@task(name="Add cols with {schema}")
def add_cols_with_schema(df, schema):
    """
    Add columns to DataFrame based on schema

    Args:
        df: dataframe
        schema: dictionary with column names as keys and types as values

    Returns:
        Dataframe with added columns
    """

    for col, type in schema.items():
        if col not in df.columns:
            df[col] = np.nan

        if type == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif type == "int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        elif type == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
        elif type == "object":
            df[col] = df[col].astype(object)
        elif type == "bool":
            df[col] = df[col].astype(bool).fillna(False)

    return df


def keep_data_to_pre_class(df, col_add_final, col_filter):
    """
    Keep data to pre classify

    Args:
        df: dataframe
        col_add_final: column add final
        col_filter: column filter

    Returns:
        Dataframe with data to pre classify
    """
    return df[(df[col_add_final] == False) & (df[col_filter])].reset_index(drop=True)


@task(name="Process Incidents Railway")
def process_incidents_railway(df_filter):
    """
    Process Incidents Railway
    """

    # get Pre classify data
    df_pre_classify = read_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_railway")

    # keep data who not add final and filter
    df_filter = keep_data_to_pre_class(
        df_filter, "add_final_inc_railway", "filter_inc_railway"
    )

    # pre_cassify with IA
    df_to_class = pre_classify_with_ia(
        df_filter, "filter_inc_railway", "add_final_inc_railway"
    )

    # pre classify Region
    df_to_class = pre_classify_region(df_to_class)

    # pre classify Applied Laws
    df_to_class = pre_classify_app_laws(df_to_class)

    # remove cols filter
    df_to_class = remove_cols_filter(df_to_class)

    # add spe cols railways
    df_to_class = add_cols_with_schema(df_to_class, SCHEMA_PRE_CLASS_RAILWAY)

    # concat data
    df = concat_old_new_df(df_raw=df_pre_classify, df_new=df_to_class, cols=["ID"])

    # save data
    save_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_railway", df)


@task(name="Process Incidents Arrest")
def process_incidents_arrest(df_filter):
    """
    Process Incidents Arrest
    """

    # get Pre classify data
    df_pre_classify = read_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_arrest")

    # keep data who not add final and filter
    df_filter = keep_data_to_pre_class(
        df_filter, "add_final_inc_arrest", "filter_inc_arrest"
    )

    # pre_cassify with IA
    df_to_class = pre_classify_with_ia(
        df_filter, "filter_inc_arrest", "add_final_inc_arrest"
    )

    # pre classify Region
    df_to_class = pre_classify_region(df_to_class)

    # pre classify Applied Laws
    df_to_class = pre_classify_app_laws(df_to_class)

    # remove cols filter
    df_to_class = remove_cols_filter(df_to_class)

    # add spe cols arrest
    df_to_class = add_cols_with_schema(df_to_class, SCHEMA_PRE_CLASS_ARREST)

    # concat data
    df = concat_old_new_df(df_raw=df_pre_classify, df_new=df_to_class, cols=["ID"])

    # save data
    save_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_arrest", df)


@task(name="Process Incidents Sabotage")
def process_incidents_sabotage(df_filter):
    """
    Process Incidents Sabotage
    """

    # get Pre classify data
    df_pre_classify = read_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_sabotage")

    # keep data who not add final and filter
    df_filter = keep_data_to_pre_class(
        df_filter, "add_final_inc_sabotage", "filter_inc_sabotage"
    )

    # pre_cassify with IA
    df_to_class = pre_classify_with_ia(
        df_filter, "filter_inc_sabotage", "add_final_inc_sabotage"
    )

    # pre classify Region
    df_to_class = pre_classify_region(df_to_class)

    # pre classify Applied Laws
    df_to_class = pre_classify_app_laws(df_to_class)

    # remove cols filter
    df_to_class = remove_cols_filter(df_to_class)

    # add spe cols sabotage
    df_to_class = add_cols_with_schema(df_to_class, SCHEMA_PRE_CLASS_SABOTAGE)

    # concat data
    df = concat_old_new_df(df_raw=df_pre_classify, df_new=df_to_class, cols=["ID"])

    # save data
    save_data(PATH_PRE_CLASSIFY_SOCIAL_MEDIA, "pre_classify_sabotage", df)


@flow(name="Process Social Media Pre Classify", log_prints=True)
def job_social_media_pre_classify():
    """
    Process filter
    """
    # get Filter data
    df_filter = read_data(PATH_FILTER_SOCIAL_MEDIA, "filter_social_media")

    # Incidents Railway
    process_incidents_railway(df_filter)

    # Incidents Arrest
    process_incidents_arrest(df_filter)

    # Incidents Sabotage
    process_incidents_sabotage(df_filter)
