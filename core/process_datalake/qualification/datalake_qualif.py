import os
import re
import pandas as pd
from tqdm import tqdm
from prefect import flow, task


# Functions
from core.libs.utils import (
    get_regions_geojson,
    concat_old_new_df,
    read_data,
    save_data,
    upd_data_artifact,
    create_artifact,
)

from core.libs.ollama_ia import ia_treat_message

# Variables
from core.config.paths import (
    PATH_FILTER_DATALAKE,
    PATH_QUALIF_DATALAKE,
    PATH_SCRIPT_SERVICE_OLLAMA,
)
from core.config.variables import (
    SIZE_TO_QUALIF,
    DICT_LAWS,
)
from core.config.schemas import (
    SCHEMA_QUALIF_RAILWAY,
    SCHEMA_QUALIF_ARREST,
    # SCHEMA_QUALIF_SABOTAGE,
)


@task(name="Qualif with IA", task_run_name="pre-classify-with-ia")
def qualif_with_ia(df, col, prompt):
    """
    Qualif specific column with IA

    Args:
        df: dataframe
        col: column to qualif
        prompt: prompt for IA

    Returns:
        Dataframe with Qualif data
    """

    group_size = 10
    df_result = pd.DataFrame()

    # Calcul number of batches
    total_batches = (len(df) + group_size - 1) // group_size

    # Create progress bar
    with tqdm(total=total_batches, desc=f"Qualif {col} with IA") as pbar:
        for i in range(0, len(df), group_size):
            df_group = df[i : i + group_size]

            # ask IA
            df_group.loc[:, col] = df_group["text_translate"].apply(
                lambda x: ia_treat_message(x, "classify", prompt)
            )

            df_result = pd.concat([df_result, df_group])

            # Update progress bar
            pbar.update(1)

    return df_result


@task(name="Qualif theme", task_run_name="pre-classify-theme")
def qualif_theme(df, theme):
    """
    Qualif theme with IA, find if message concern a theme

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with Qualif data
    """

    if theme == "railway":
        prompt = """
        You are an expert in text analysis and classification.

        You do respond by YES or NO 
        does the message concern a railway incident ?

        A message who concern a railway incident is a message that contains information about accidents, incidents, derailments, sabotage, arson, vandalism, fire, arrests, sentences, or any other event that occurs on a railway.

        Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with YES or NO.
        """
    elif theme == "arrest":
        prompt = """
        You are an expert in text analysis and classification.
        
        You do respond by YES or NO
        does the message concern an arrest or sentence ?
        
        A message who concern an arrest or sentence is a message that contains information about arrests, sentences, trials, court decisions, or any other event that involves the arrest or sentencing of an individual.
        
        Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with YES or NO.
        """
    # elif theme == "sabotage":
    #     prompt = """
    #     You are an expert in text analysis and classification.

    #     You do respond by YES or NO
    #     does the message concern sabotage, arson, vandalism, or acts against Russian infrastructure or government ?

    #     A message who concern sabotage, arson, vandalism, or acts against Russian infrastructure or government is a message that contains information about sabotage, arson, vandalism, acts against Russian infrastructure or government, or any other event that involves sabotage, arson, vandalism, or acts against Russian infrastructure or government.

    #     Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with YES or NO.
    #     """

    # qualif with IA
    df = qualif_with_ia(df, "qualif_ia", prompt)

    return df


@task(name="Qualif Partisans Names", task_run_name="pre-classify-partisans-names")
def qualif_partisans_names(df, theme):
    """
    Find partisans names in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with Qualif partisans names
    """

    # add col
    if theme == "railway":
        col_name = "qualif_prtsn_names"
    elif theme == "arrest":
        col_name = "qualif_person_name"

    prompt = """
    If available, please provide the name(s) of the individual(s) who have been arrested or sentenced. Kindly respond with only the names, as you are not required to discuss the content of the message.
    If the message contains multiple names, please provide all of them.
    If the message does not contain any names, please respond with "No names".
    
    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with names or "No names".
    """

    # Find partisans names
    return qualif_with_ia(df, col_name, prompt)


@task(name="Qualif Partisans Ages", task_run_name="pre-classify-partisans-ages")
def qualif_partisans_ages(df, theme):
    """
    Find partisans ages in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with Qualif partisans ages
    """

    # add col
    if theme == "railway":
        col_age = "qualif_prtsn_age"
    elif theme == "arrest":
        # col_age = "qualif_person_age"
        return df

    # prompt
    prompt = """
    If available, please provide the age(s) of the individual(s) who have been arrested or sentenced. Kindly respond with only the ages, as you are not required to discuss the content of the message.
    If the message contains multiple ages, please provide all of them.
    If the message does not contain any ages, please respond with "No ages".

    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with ages or "No ages".
    """

    # Find partisans ages
    return qualif_with_ia(df, col_age, prompt)


@task(name="Qualif Incident Type", task_run_name="pre-classify-incident-type")
def qualif_incident_type(df, theme):
    """
    Find incident type in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with Qualif incident type
    """

    # add col
    if theme == "railway":
        col_age = "qualif_inc_type"
    elif theme == "arrest":
        return df

    # prompt
    prompt = """
    If available, please provide the type of incident that occurred. Kindly respond with only the type of incident, as you are not required to discuss the content of the message.
    
    You can choose from the following options: Derailment, Sabotage, Fire, Collision, Attack, Other.
    
    If the message contains multiple types of incidents, please provide only one, the most relevant if possible.
    If the message does not contain any information about the type of incident, please respond with "Other".
    
    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with the type of incident or "Other".
    """

    # Find Incident Type
    return qualif_with_ia(df, col_age, prompt)


@task(
    name="Qualif Damaged Equipment",
    task_run_name="pre-classify-damaged-equipment",
)
def qualif_damaged_equipment(df, theme):
    """
    Find damaged equipment in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with Qualif damaged equipment
    """

    # add col
    if theme == "railway":
        col_age = "qualif_dmg_eqp"
    elif theme == "arrest":
        return df

    # prompt
    prompt = """
    If available, please provide the type of equipment that was damaged. Kindly respond with only the type of equipment, as you are not required to discuss the content of the message.
    
    You can choose from the following options: Freight Train, Passengers Train, Locomotive, Relay Cabin, Infrastructure, Railroad Tracks, Electric Box, Unknown.
    
    If the message contains multiple types of equipment, please provide only one, the most relevant if possible.
    If the message does not contain any information about the type of equipment, please respond with "Unknown".
    
    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with the type of equipment or "Unknown".
    """

    # Find Damaged Equipment
    return qualif_with_ia(df, col_age, prompt)


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


@task(name="Qualif Region", task_run_name="pre-classify-region")
def qualif_region(df):
    """
    Qualif Region

    Args:
        df: dataframe

    Returns:
        Dataframe with Qualif region
    """

    # get regions
    dict_regions = get_regions_geojson()

    # put keys in list
    LIST_REGIONS = list(dict_regions.keys())

    df.loc[:, "qualif_region"] = df["text_translate"].apply(
        lambda x: find_region(x, LIST_REGIONS)
    )

    return df


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


@task(name="Qualif Applied Laws", task_run_name="pre-classify-applied-laws")
def qualif_app_laws(df):
    """
    Qualif Applied Laws

    Args:
        df: dataframe

    Returns:
        Dataframe with Qualif applied laws
    """

    df.loc[:, "qualif_app_laws"] = df["text_translate"].apply(
        lambda x: find_law(x, DICT_LAWS)
    )

    return df


@task(name="Remove cols filter", task_run_name="remove-cols-filter")
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


@task(name="Add cols with Schema", task_run_name="add-cols-with-schema")
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
            df[col] = None

        if type == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif type == "int64":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        elif type == "float64":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        elif type == "object":
            df[col] = df[col].astype(object)
        elif type == "bool":
            df[col] = df[col].astype(bool).fillna(False)

    return df


@task(name="Keep data to qualif", task_run_name="keep-data-to-pre-classify")
def keep_data_to_qualif(df_filt, col_add_final, col_filter):
    """
    Keep data who "add_final" is False and "filter" is True

    Args:
        df_filt: dataframe with filter data
        col_add_final: column add final
        col_filter: column filter

    Returns:
        Dataframe with data to qualif
    """

    # keep data according to cols
    return df_filt[
        (df_filt[col_add_final] == False) & (df_filt[col_filter])
    ].reset_index(drop=True)


@flow(
    name="Flow Single qualification {theme}",
    flow_run_name="flow-single-pre-classification-{theme}",
    log_prints=True,
)
def process_qualification(theme, df_filt):
    """
    Apply qualifications
    """

    # init vars
    if theme == "railway":
        file_name = "qualification_railway"
        schema = SCHEMA_QUALIF_RAILWAY
        col_add_final = "add_final_inc_railway"
        col_filter = "filter_inc_railway"
    elif theme == "arrest":
        file_name = "qualification_arrest"
        schema = SCHEMA_QUALIF_ARREST
        col_add_final = "add_final_inc_arrest"
        col_filter = "filter_inc_arrest"
    # elif theme == "sabotage":
    #     file_name = "qualification_sabotage"
    #     schema = SCHEMA_QUALIF_SABOTAGE
    #     col_add_final = "add_final_inc_sabotage"
    #     col_filter = "filter_inc_sabotage"

    # get data already Qualif
    df_qualif = read_data(PATH_QUALIF_DATALAKE, file_name)

    # keep data who not add final and filter
    df_to_class = keep_data_to_qualif(df_filt, col_add_final, col_filter)

    df_ids_no_qualif_ia = pd.DataFrame()

    # if not df_qualif.empty:
    #     # some data already Qualif
    #     # get list ID not processed with IA
    #     df_ids_no_qualif_ia = df_to_class[
    #         ~df_to_class["ID"].isin(df_qualif["ID"])  # new data
    #         | (
    #             df_to_class["ID"].isin(df_qualif["ID"])  # no qualif ia
    #             # & df_qualif["qualif_ia"].isnull()
    #             & pd.isnull(df_qualif["qualif_ia"])
    #         )
    #         & (df_to_class[col_add_final] == False)
    #         & (df_to_class[col_filter])
    #     ]["ID"]

    if not df_qualif.empty:
        # merge filter and qualif
        df_merged = pd.merge(
            df_to_class, df_qualif[["ID", "qualif_ia"]], on="ID", how="left"
        )

        # get list ID not processed with IA
        df_ids_no_qualif_ia = df_merged[
            (~df_merged["ID"].isin(df_qualif["ID"]))  # New IDs
            | (pd.isnull(df_merged["qualif_ia"]))  # IDs without qualif_ia
            & (df_merged[col_add_final] == False)
            & (df_merged[col_filter])
        ]["ID"]

        if df_ids_no_qualif_ia.empty:
            print("End of process, because no new data and all data already processed")
            return

        # keep data not in qualif
        df_to_class = df_to_class[~df_to_class["ID"].isin(df_qualif["ID"])].reset_index(
            drop=True
        )

    """
    Qualif without IA
    """
    print("WITHOUT IA")
    print("Data to classify: ", df_to_class.shape)
    if not df_to_class.empty:

        # qualif Region
        df_to_class = qualif_region(df_to_class)

        # qualif Applied Laws
        df_to_class = qualif_app_laws(df_to_class)

        # remove cols filter
        df_to_class = remove_cols_filter(df_to_class)

        # add cols with schema
        df_to_class = add_cols_with_schema(df_to_class, schema)

        # concat old data
        df_to_class = concat_old_new_df(
            df_raw=df_qualif, df_new=df_to_class, cols=["ID", "IDX"]
        )

    """
    Qualif with IA
    """
    print("WITH IA")
    if df_to_class.empty:
        df_to_class = df_qualif.copy()

    # merge filter and qualif
    df_to_class = pd.merge(
        df_to_class,
        df_filt[["ID", "text_original", "text_translate"]],
        on=["ID"],
        how="left",
        suffixes=("", "_y"),
    )

    if not df_ids_no_qualif_ia.empty:
        # keep data not qualify with IA and not classifed
        df_to_class_wh_ia = df_to_class[
            (df_to_class["ID"].isin(df_ids_no_qualif_ia))
        ].reset_index(drop=True)
    else:
        df_to_class_wh_ia = df_to_class

    print("Data to classify: ", df_to_class_wh_ia.shape)

    # keep data according to SIZE_TO_QUALIF
    # We define a threshold of x to avoid excessive processing time due to the IA
    if SIZE_TO_QUALIF:
        df_to_class_wh_ia = df_to_class_wh_ia.head(SIZE_TO_QUALIF)

    # process IA
    print("Data to classify after SIZE_TO_QUALIF: ", df_to_class_wh_ia.shape)

    df_to_class_wh_ia = qualif_theme(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = qualif_partisans_names(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = qualif_partisans_ages(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = qualif_incident_type(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = qualif_damaged_equipment(df_to_class_wh_ia, theme)

    # concat data
    df_to_class = (
        pd.concat([df_to_class, df_to_class_wh_ia])
        .drop_duplicates(subset=["ID", "IDX"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # remove text_translate
    df_to_class = df_to_class.drop(columns=["text_original", "text_translate"])

    # update types
    df_to_class = df_to_class.astype(schema)

    # save data
    save_data(PATH_QUALIF_DATALAKE, file_name, df_to_class)


@flow(
    name="Flow Master Datalake Qualif",
    flow_run_name="flow-master-datalake-pre-classify",
    log_prints=True,
)
def flow_datalake_qualif():
    """
    Process Qualif Datalake
    """

    # start service ollama
    os.system(f"sh {PATH_SCRIPT_SERVICE_OLLAMA}")

    # get Filter data
    df_filter = read_data(PATH_FILTER_DATALAKE, "filter_datalake")

    # Incidents Railway
    process_qualification("railway", df_filter)

    # Incidents Arrest
    process_qualification("arrest", df_filter)

    # Incidents Sabotage
    # process_qualification("sabotage", df_filter)

    # create artifact
    # create_artifact("flow-master-datalake-pre-classify-artifact")
