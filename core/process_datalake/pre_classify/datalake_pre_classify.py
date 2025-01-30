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
    PATH_PRE_CLASSIFY_DATALAKE,
    PATH_SCRIPT_SERVICE_OLLAMA,
)
from core.config.variables import (
    SIZE_TO_PRE_CLASSIFY,
    DICT_LAWS,
)
from core.config.schemas import (
    SCHEMA_PRE_CLASS_RAILWAY,
    SCHEMA_PRE_CLASS_ARREST,
    SCHEMA_PRE_CLASS_SABOTAGE,
)


@task(name="Pre classify with IA", task_run_name="pre-classify-with-ia")
def pre_classify_with_ia(df, col, prompt):
    """
    Pre classify specific column with IA

    Args:
        df: dataframe
        col: column to pre classify
        prompt: prompt for IA

    Returns:
        Dataframe with pre classified data
    """

    group_size = 10
    df_result = pd.DataFrame()

    # Calcul number of batches
    total_batches = (len(df) + group_size - 1) // group_size

    # Create progress bar
    with tqdm(total=total_batches, desc=f"Pre classify {col} with IA") as pbar:
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


@task(name="Pre classify theme", task_run_name="pre-classify-theme")
def pre_classify_theme(df, theme):
    """
    Pre classify theme with IA, find if message concern a theme

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with pre classified data
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
    elif theme == "sabotage":
        prompt = """
        You are an expert in text analysis and classification.
        
        You do respond by YES or NO
        does the message concern sabotage, arson, vandalism, or acts against Russian infrastructure or government ?
        
        A message who concern sabotage, arson, vandalism, or acts against Russian infrastructure or government is a message that contains information about sabotage, arson, vandalism, acts against Russian infrastructure or government, or any other event that involves sabotage, arson, vandalism, or acts against Russian infrastructure or government.
        
        Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with YES or NO.
        """

    # pre classify with IA
    df = pre_classify_with_ia(df, "pre_class_ia", prompt)

    return df


@task(name="Pre classify Partisans Names", task_run_name="pre-classify-partisans-names")
def pre_classify_partisans_names(df, theme):
    """
    Find partisans names in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with pre classified partisans names
    """

    # add col
    if theme == "railway":
        col_name = "pre_class_prtsn_names"
    elif theme == "arrest":
        col_name = "pre_class_person_name"
    elif theme == "sabotage":
        col_name = "pre_class_prtsn_names"

    prompt = """
    If available, please provide the name(s) of the individual(s) who have been arrested or sentenced. Kindly respond with only the names, as you are not required to discuss the content of the message.
    If the message contains multiple names, please provide all of them.
    If the message does not contain any names, please respond with "No names".
    
    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with names or "No names".
    """

    # Find partisans names
    return pre_classify_with_ia(df, col_name, prompt)


@task(name="Pre classify Partisans Ages", task_run_name="pre-classify-partisans-ages")
def pre_classify_partisans_ages(df, theme):
    """
    Find partisans ages in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with pre classified partisans ages
    """

    # add col
    if theme == "railway":
        col_age = "pre_class_prtsn_age"
    elif theme == "arrest":
        col_age = "pre_class_person_age"
    elif theme == "sabotage":
        col_age = "pre_class_prtsn_age"

    # prompt
    prompt = """
    If available, please provide the age(s) of the individual(s) who have been arrested or sentenced. Kindly respond with only the ages, as you are not required to discuss the content of the message.
    If the message contains multiple ages, please provide all of them.
    If the message does not contain any ages, please respond with "No ages".

    Please, give only the response, If the message is ambiguous, base your answer on the most likely clues in the text. Do not ask questions or provide additional explanations in your answer. Answer only with ages or "No ages".
    """

    # Find partisans ages
    return pre_classify_with_ia(df, col_age, prompt)


@task(name="Pre classify Incident Type", task_run_name="pre-classify-incident-type")
def pre_classify_incident_type(df, theme):
    """
    Find incident type in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with pre classified incident type
    """

    # add col
    if theme == "railway":
        col_age = "pre_class_inc_type"
    elif theme == "arrest":
        return df
    elif theme == "sabotage":
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
    return pre_classify_with_ia(df, col_age, prompt)


@task(
    name="Pre classify Damaged Equipment",
    task_run_name="pre-classify-damaged-equipment",
)
def pre_classify_damaged_equipment(df, theme):
    """
    Find damaged equipment in text with IA

    Args:
        df: dataframe
        theme: theme to classify

    Returns:
        Dataframe with pre classified damaged equipment
    """

    # add col
    if theme == "railway":
        col_age = "pre_class_dmg_eqp"
    elif theme == "arrest":
        return df
    elif theme == "sabotage":
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
    return pre_classify_with_ia(df, col_age, prompt)


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


@task(name="Pre classify Region", task_run_name="pre-classify-region")
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

    df.loc[:, "pre_class_region"] = df["text_translate"].apply(
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


@task(name="Pre classify Applied Laws", task_run_name="pre-classify-applied-laws")
def pre_classify_app_laws(df):
    """
    Pre classify Applied Laws

    Args:
        df: dataframe

    Returns:
        Dataframe with pre classified applied laws
    """

    df.loc[:, "pre_class_app_laws"] = df["text_translate"].apply(
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
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
        elif type == "object":
            df[col] = df[col].astype(object)
        elif type == "bool":
            df[col] = df[col].astype(bool).fillna(False)

    return df


@task(name="Keep data to pre classify", task_run_name="keep-data-to-pre-classify")
def keep_data_to_pre_class(df_filt, col_add_final, col_filter):
    """
    Keep data who "add_final" is False and "filter" is True

    Args:
        df_filt: dataframe with filter data
        col_add_final: column add final
        col_filter: column filter

    Returns:
        Dataframe with data to pre classify
    """

    # keep data according to cols
    return df_filt[
        (df_filt[col_add_final] == False) & (df_filt[col_filter])
    ].reset_index(drop=True)


@flow(
    name="Flow Single Pre Classification {theme}",
    flow_run_name="flow-single-pre-classification-{theme}",
    log_prints=True,
)
def process_pre_classification(theme, df_filt):
    """
    Apply Pre Classifications
    """

    # init vars
    if theme == "railway":
        file_name = "pre_classify_railway"
        schema = SCHEMA_PRE_CLASS_RAILWAY
        col_add_final = "add_final_inc_railway"
        col_filter = "filter_inc_railway"
    elif theme == "arrest":
        file_name = "pre_classify_arrest"
        schema = SCHEMA_PRE_CLASS_ARREST
        col_add_final = "add_final_inc_arrest"
        col_filter = "filter_inc_arrest"
    elif theme == "sabotage":
        file_name = "pre_classify_sabotage"
        schema = SCHEMA_PRE_CLASS_SABOTAGE
        col_add_final = "add_final_inc_sabotage"
        col_filter = "filter_inc_sabotage"

    # get data already Pre classify
    df_pre_class = read_data(PATH_PRE_CLASSIFY_DATALAKE, file_name)
    print("\n", df_pre_class.head(30))

    # keep data who not add final and filter
    df_to_class = keep_data_to_pre_class(df_filt, col_add_final, col_filter)
    print("\n", df_to_class.head(30))

    df_ids_no_pre_class_ia = pd.DataFrame()

    # if not df_pre_class.empty:
    #     # some data already pre classified
    #     # get list ID not processed with IA
    #     df_ids_no_pre_class_ia = df_to_class[
    #         ~df_to_class["ID"].isin(df_pre_class["ID"])  # new data
    #         | (
    #             df_to_class["ID"].isin(df_pre_class["ID"])  # no pre_class ia
    #             # & df_pre_class["pre_class_ia"].isnull()
    #             & pd.isnull(df_pre_class["pre_class_ia"])
    #         )
    #         & (df_to_class[col_add_final] == False)
    #         & (df_to_class[col_filter])
    #     ]["ID"]

    if not df_pre_class.empty:
        # merge filter and pre classify
        df_merged = pd.merge(
            df_to_class, df_pre_class[["ID", "pre_class_ia"]], on="ID", how="left"
        )
        print("DFDFVDFVDVDVDFVDFVDFV\n", df_merged.head(50))

        # get list ID not processed with IA
        df_ids_no_pre_class_ia = df_merged[
            (~df_merged["ID"].isin(df_pre_class["ID"]))  # New IDs
            | (pd.isnull(df_merged["pre_class_ia"]))  # IDs without pre_class_ia
            & (df_merged[col_add_final] == False)
            & (df_merged[col_filter])
        ]["ID"]

        if df_ids_no_pre_class_ia.empty:
            print("End of process, because no new data and all data already processed")
            return

        # keep data not in pre classify
        df_to_class = df_to_class[
            ~df_to_class["ID"].isin(df_pre_class["ID"])
        ].reset_index(drop=True)

    """
    Pre Classify without IA
    """
    if not df_to_class.empty:

        # pre classify Region
        df_to_class = pre_classify_region(df_to_class)

        # pre classify Applied Laws
        df_to_class = pre_classify_app_laws(df_to_class)

        # remove cols filter
        df_to_class = remove_cols_filter(df_to_class)

        # add cols with schema
        df_to_class = add_cols_with_schema(df_to_class, schema)

        # concat old data
        df_to_class = concat_old_new_df(
            df_raw=df_pre_class, df_new=df_to_class, cols=["ID", "IDX"]
        )

    print("LIST ID")
    print("\n", df_ids_no_pre_class_ia.head(30))

    print("TO CLASSIFY")
    print("\n", df_to_class.head(30))
    """
    Pre Classify with IA
    """

    if df_to_class.empty:
        df_to_class = df_pre_class.copy()

    # merge filter and pre classify
    df_to_class = pd.merge(
        df_to_class,
        df_filt[["ID", "text_original", "text_translate"]],
        on=["ID"],
        how="left",
        suffixes=("", "_y"),
    )

    if not df_ids_no_pre_class_ia.empty:
        # keep data not pre_call with IA and not classifed
        df_to_class_wh_ia = df_to_class[
            (df_to_class["ID"].isin(df_ids_no_pre_class_ia))
        ].reset_index(drop=True)
    else:
        df_to_class_wh_ia = df_to_class

    # keep data according to SIZE_TO_PRE_CLASSIFY
    # We define a threshold of x to avoid excessive processing time due to the IA
    if SIZE_TO_PRE_CLASSIFY:
        df_to_class_wh_ia = df_to_class_wh_ia.head(SIZE_TO_PRE_CLASSIFY)

    print("WITH IA")
    print("\n", df_to_class_wh_ia.head(30))

    # process IA
    df_to_class_wh_ia = pre_classify_theme(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = pre_classify_partisans_names(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = pre_classify_partisans_ages(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = pre_classify_incident_type(df_to_class_wh_ia, theme)
    df_to_class_wh_ia = pre_classify_damaged_equipment(df_to_class_wh_ia, theme)
    print("\n", df_to_class_wh_ia.head(30))

    # concat data
    df_to_class = (
        pd.concat([df_to_class, df_to_class_wh_ia])
        .drop_duplicates(subset=["ID", "IDX"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # remove text_translate
    df_to_class = df_to_class.drop(columns=["text_original", "text_translate"])
    print("\n", df_to_class)
    print("\n", df_to_class.dtypes)

    # save data
    save_data(PATH_PRE_CLASSIFY_DATALAKE, file_name, df_to_class)


@flow(
    name="Flow Master Datalake Pre Classify",
    flow_run_name="flow-master-datalake-pre-classify",
    log_prints=True,
)
def job_datalake_pre_classify():
    """
    Process Pre Classify Datalake
    """

    print("********************************")
    print("Start Pre Classifications")
    print("********************************")

    # start service ollama
    os.system(f"sh {PATH_SCRIPT_SERVICE_OLLAMA}")

    # get Filter data
    df_filter = read_data(PATH_FILTER_DATALAKE, "filter_datalake")

    # Incidents Railway
    process_pre_classification("railway", df_filter)

    # Incidents Arrest
    # process_pre_classification("arrest", df_filter)

    # Incidents Sabotage
    # process_pre_classification("sabotage", df_filter)

    # create artifact
    # create_artifact("flow-master-datalake-pre-classify-artifact")
