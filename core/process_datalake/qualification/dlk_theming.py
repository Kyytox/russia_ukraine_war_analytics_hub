import os
from dotenv import load_dotenv
import datetime

import re
import pandas as pd
from prefect import flow, task
from typesafe_sdk import TypeSafeClient


# Functions
from core.libs.utils import create_artifact


from core.config.variables import SIZE_THEME_JEV_AI, ACCEPT_VALID_NOUL

from core.utils.terms_filter.terms_incidents_railway import (
    list_words_set_railway,
    list_substr_set_railway,
    list_expression_railways,
    list_word_railways,
)

from core.utils.terms_filter.terms_arrest import (
    list_words_set_arrest,
    list_substr_set_arrest,
    list_expression_arrest,
    list_word_arrest,
)

from core.utils.terms_filter.terms_sabotage import (
    list_words_set_sabotage,
    list_substr_set_sabotage,
    list_expression_sabotage,
    list_word_sabotage,
)

from core.libs.duckdb_utils import get_data_into_duckdb, update_messages_theme_in_duckdb

load_dotenv()


def find_terms_in_text(list_terms, text, id_filter):
    """
    Find terms in text with error handling

    Args:
        list_terms: list of terms to find
        text: text
        id_filter: filter type (1, 2, or 3)

    Returns:
        Tuple (True if term in text, list of found terms)
    """

    try:
        match id_filter:
            case 1:
                for word_set in list_terms:
                    if all(re.search(rf"\b{word}\b", text) for word in word_set):
                        return True, ",".join(word_set)
            case 2:
                for word_set in list_terms:
                    if all(word in text for word in word_set):
                        return True, ",".join(word_set)
            case 3:
                found_terms = [
                    term
                    for term in list_terms
                    if re.search(rf"\b{re.escape(term)}\b", text, re.IGNORECASE)
                ]
                # return bool(found_terms), ",".join(found_terms) if found_terms else None
                if found_terms:
                    return True, ",".join(found_terms)
                else:
                    return None, None

    except re.error as e:
        print(f"Regex error: {e}")
        return None, None

    return None, None


@task(name="Theming with terms", task_run_name="theming-with-terms")
def theming_with_terms(df, railway_terms_lists, col_theme, col_terms):
    """
    Task Theming with terms

    Args:
        df: dataframe with data to process
        railway_terms_lists: list of terms lists to use for theming
        col_theme: column name for theme
        col_terms: column name for terms found

    Returns:
        Dataframe with theme and terms columns updated

    """

    # Part 1: Apply theming based on terms lists
    if not df.empty:
        for terms, id_filter in railway_terms_lists:
            # Process only rows where theme_inc_rail is null AND text_translate is not null
            mask = (df[col_theme].isna()) & pd.notna(df["text_translate"])
            print(f"Data mask to filter: {df[mask].shape}")

            results = df.loc[mask, "text_translate"].apply(
                lambda x: find_terms_in_text(
                    terms,
                    x,
                    id_filter=id_filter,
                )
            )

            # Zip results
            found_any_list, found_terms_list = zip(*results)

            # Update the theme_inc_rail column only for null values
            df.loc[mask, col_theme] = list(found_any_list)
            df.loc[mask, col_terms] = list(found_terms_list)
    else:
        print("No data to process for railway incident theming")

    # put theme and terms cols to False for all None
    df[col_theme] = df[col_theme].fillna(False)
    df[col_terms] = df[col_terms].fillna(None)

    # Summary counts
    print(f"total data True found: {df[col_theme].sum()}")
    print(f"total data False found: {(~df[col_theme]).sum()}")
    print("df shape: ", df.shape)

    return df


@task(name="Theming with JEV AI", task_run_name="theming-with-jev-ai")
def theming_with_jev_ai(df, type_theme, col_theme, col_terms):
    """
    Task Theming with JEV AI

    Args:
        df: dataframe with data to process
        type_theme: type of theme to process
        col_theme: column name for theme
        col_terms: column name for terms found

    Returns:
        Dataframe with theme and terms columns updated
    """

    # Get JEV API key from environment variable
    jev_api_key = os.getenv("TYPESAFE_API_KEY")

    remaining_mask = (df[col_theme] == True) & pd.notna(df["text_translate"])
    remaining_indices = df[remaining_mask].index.tolist()

    print(f"\nStarting JEV AI classification for {len(remaining_indices)} messages")
    batch_size = SIZE_THEME_JEV_AI
    ai_matched_total = 0
    ai_errors_total = 0
    cpt = 0

    match type_theme:
        case "incident_railway":
            question_jev = {
                type_theme: {
                    "type": "noul",
                    "instructions": "Is this message about a railway incident in Russia? (accidents, collisions, derailments, sabotage, disruptions, etc.)",
                    "criteria": {
                        "true": "Message mentions railway accidents, collisions, derailments, sabotage, disruptions, or other railway-related incidents in Russia",
                        "false": "No mention of any railway incidents",
                    },
                }
            }
        case "arrest":
            question_jev = {}
        case "sabotage":
            question_jev = {}

    # exit()

    if len(remaining_indices) > 0:
        # Process batches
        print(f"Processing by batches of {batch_size} messages")
        for batch_num in range(0, len(remaining_indices), batch_size):
            batch_start = batch_num
            batch_end = min(batch_num + batch_size, len(remaining_indices))
            batch_indices = remaining_indices[batch_start:batch_end]

            batch = batch_num // batch_size + 1
            print(f"\n--- Batch {batch}: Processing {len(batch_indices)} messages ---")

            ai_matched = 0
            ai_errors = 0
            batch_results = []

            # Process each message in the batch
            for idx in batch_indices:
                text = df.loc[idx, "text_translate"]

                if cpt % 50 == 0:
                    print(f"Processing message {cpt}")

                try:
                    with TypeSafeClient(api_key=jev_api_key) as client:
                        response = client.system_one(
                            state={"state": text}, questions=question_jev
                        )

                    # Check if the response is valid
                    is_incident = response.nouls[type_theme].noul > ACCEPT_VALID_NOUL

                    # Update the DataFrame with the classification result
                    if is_incident:
                        df.loc[idx, col_theme] = is_incident
                        df.loc[idx, col_terms] = (
                            f"JEV IA Result {response.nouls[type_theme].noul:.2f}"
                        )
                        ai_matched += 1
                    else:
                        df.loc[idx, col_theme] = is_incident
                        df.loc[idx, col_terms] = (
                            f"JEV IA Result {response.nouls[type_theme].noul:.2f}"
                        )

                    batch_results.append(
                        {
                            "id_message": df.loc[idx, "id_message"],
                            col_theme: is_incident,
                            col_terms: df.loc[idx, col_terms],
                        }
                    )

                    cpt += 1

                except Exception as e:
                    print(f"  ✗ Error classifying message {idx}: {e}")
                    print(f"  ✗ Message ID: {df.loc[idx, 'id_message']}")
                    print(f"  ✗ Message text (8000): {text[:8000]}")
                    ai_errors += 1

            ai_matched_total += ai_matched
            ai_errors_total += ai_errors
            print(f"Batch completed: {ai_matched} matches, {ai_errors} errors")

            # Update DuckDB with batch results
            if batch_results:
                update_messages_theme_in_duckdb(pd.DataFrame(batch_results))
    else:
        print("No messages to process with JEV AI classification")

    print(f"Total matches: {ai_matched_total}")
    print(f"Total errors: {ai_errors_total}")

    return df


@task(
    name="Theming Incidents Railway",
    task_run_name="theming-incidents-railway",
)
def theming_incidents_railway(df):
    """
    Task Theming Incidents Railway with JEV AI Classification

    Args:
        df: dataframe with data to process

    Returns:
        Dataframe with theme_inc_railway column updated
    """

    # Initial filter: keep rows not yet tagged as railway incident and with a non-null translated text
    df = df[
        (df["theme_inc_railway"].isna()) & df["text_translate"].notna()
    ].reset_index(drop=True)

    print(f"Initial df shape for incident railway theming: {df.shape}")

    # Prepare terms lists fayor railway incidents
    railway_terms_lists = [
        (list_words_set_railway, 1),
        (list_substr_set_railway, 2),
        (list_expression_railways, 3),
        (list_word_railways, 3),
    ]

    # data artifact
    data_art = []
    data_art.append({"key": "Data to process ", "value": df.shape[0]})

    #
    # Part 1: Apply theming based on terms lists
    df = theming_with_terms(
        df,
        railway_terms_lists,
        col_theme="theme_inc_railway",
        col_terms="terms_found_inc_railway",
    )

    terms_true = df[df["theme_inc_railway"] == True].shape[0]
    terms_false = df[df["theme_inc_railway"] == False].shape[0]

    #
    # Part 2: JEV AI Classification for messages where theme_inc_railway is True
    df = theming_with_jev_ai(
        df,
        type_theme="incident_railway",
        col_theme="theme_inc_railway",
        col_terms="terms_found_inc_railway",
    )

    jev_true = df[df["theme_inc_railway"] == True].shape[0]
    jev_false = df[df["theme_inc_railway"] == False].shape[0]

    # Update table in DuckDB with data wherer theme_inc_railway is False
    df_false = df[df["theme_inc_railway"] == False][
        ["id_message", "theme_inc_railway", "terms_found_inc_railway"]
    ]

    if not df_false.empty:
        update_messages_theme_in_duckdb(df_false)
    else:
        print("No messages with theme_inc_railway == False to update in DuckDB")

    # Update artifact
    data_art.append({"key": "Data themed with terms = TRUE", "value": terms_true})
    data_art.append({"key": "Data themed with terms = FALSE", "value": terms_false})
    data_art.append({"key": "Data themed with JEV AI = TRUE", "value": jev_true})
    data_art.append({"key": "Data themed with JEV AI = FALSE", "value": jev_false})

    return data_art


@flow(
    name="DLK SubFlow Theming",
    flow_run_name="dlk-subflow-theming",
    log_prints=True,
)
def subflow_datalake_theming():
    """
    SubFlow Datalake Theming
    """

    # Get data
    query = """
    SELECT 
        msg.id_message, 
        msg.date_message,
        msg.text_translate,
        msg_th.theme_inc_railway,
        msg_th.theme_arrest,
        msg_th.theme_sabotage,
        msg_th.terms_found_inc_railway,
        msg_th.terms_found_arrest,
        msg_th.terms_found_sabotage
    FROM main.messages as msg
    LEFT JOIN main.messages_theme as msg_th USING (id_message)
    ORDER BY msg.date_message ASC
    """

    df = get_data_into_duckdb(query)

    df = df[df["date_message"] > datetime.datetime(2024, 4, 1)]  # for tests

    # Apply theming for railway incidents
    data_art_inc_rail = theming_incidents_railway(df)

    # create artifact
    create_artifact("dlk-theming-incident-railway-art", "table", data_art_inc_rail)
