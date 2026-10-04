import os
from dotenv import load_dotenv
import re
import pandas as pd
from tqdm import tqdm
from prefect import flow, task

from typesafe_sdk import TypeSafeClient, NoulCriteria, Noul


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

from core.config.variables import SIZE_THEME_JEV_AI, ACCEPT_VALID_NOUL
from core.libs.duckdb_utils import get_data_into_duckdb, insert_data_into_duckdb

load_dotenv()


@task(name="Qualif with Ollama IA", task_run_name="qualif-with-ollama-ia")
def qualif_with_ollama_ia(df):
    """
    Qualif with Ollama IA
    Extract multiple information at once from text with Ollama IA

    Args:
        df: dataframe

    Returns:
        Dataframe with Qualif data
    """

    # number of rows to process in each batch
    group_size = SIZE_TO_QUALIF

    # Calculate number of batches
    total_batches = (len(df) + group_size - 1) // group_size

    # Create progress bar
    with tqdm(total=total_batches, desc="Qualif with Ollama IA") as pbar:
        # Process by batches
        for batch_start in range(0, len(df), group_size):
            batch_end = min(batch_start + group_size, len(df))

            # Get batch indices
            batch_indices = df.iloc[batch_start:batch_end].index

            # Process each row in the batch
            for idx in batch_indices:
                text = df.loc[idx, "text_translate"]

                # Ask IA
                results = ia_treat_message(
                    text,
                    "qualif",
                    "Extract information about partisans_names and partisans_ages.",
                )

                # Update DataFrame directly
                df.loc[idx, "qualif_prtsn_names"] = results.get("partisans_names", None)
                df.loc[idx, "qualif_prtsn_age"] = results.get("partisans_ages", None)

            # Update progress bar (once per batch, not per row)
            pbar.update(1)

    return df


@task(name="Qualif with Jev IA", task_run_name="qualif-with-jev-ia")
def qualif_with_jev_ia(df, question_jev, dict_correspond):
    """
    Qualif with Jev IA
    Extract multiple information at once (columns) from text with Jev IA

    Args:
        df: dataframe
        question_jev: dict with questions for Jev IA
        dict_correspond: dict with correspondence between Jev IA keys and DataFrame columns

    Returns:
        Dataframe with Qualif data
    """
    # Init variables
    jev_api_key = os.getenv("TYPESAFE_API_KEY")
    ai_errors = 0
    batch_results = []

    # number of rows to process in each batch
    group_size = SIZE_TO_QUALIF
    total_batches = (len(df) + group_size - 1) // group_size

    with tqdm(total=total_batches, desc="Qualif with Jev IA") as pbar:
        for batch_start in range(0, len(df), group_size):
            batch_end = min(batch_start + group_size, len(df))
            batch_indices = df.iloc[batch_start:batch_end].index

            for index in batch_indices:
                row = df.loc[index]
                text = row["text_translate"]

                try:
                    with TypeSafeClient(api_key=jev_api_key) as client:
                        response = client.system_one(
                            state={"state": text}, questions=question_jev
                        )

                        # Update DataFrame
                        df.at[index, dict_correspond[0]["value"]] = response.answers[
                            dict_correspond[0]["key"]
                        ].choice
                        df.at[index, dict_correspond[1]["value"]] = response.answers[
                            dict_correspond[1]["key"]
                        ].choice
                        df.at[index, dict_correspond[2]["value"]] = response.answers[
                            dict_correspond[2]["key"]
                        ].choice
                        df.at[index, dict_correspond[3]["value"]] = (
                            response.answers[dict_correspond[3]["key"]].noul
                            > ACCEPT_VALID_NOUL
                        )

                except Exception as e:
                    print(f"Error processing row {index}: {e}")
                    ai_errors += 1

            # Progress bar update (par batch, pas par row)
            pbar.update(1)

    print(f"Total AI errors: {ai_errors}")

    # replace "None" by None
    df = df.replace("None", None)

    return df


@task(name="Qualif Applied Laws", task_run_name="qualif-applied-laws")
def qualif_app_laws(df):
    """
    Qualif Applied Laws

    Args:
        df: dataframe

    Returns:
        Dataframe with Qualif applied laws
    """

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
        return ", ".join(found_laws) if found_laws else None

    # get applied laws
    df.loc[:, "qualif_app_laws"] = df["text_translate"].apply(
        lambda x: find_law(x, DICT_LAWS)
    )

    return df


@task(name="Qualif Region", task_run_name="qualif-region")
def qualif_region(df):
    """
    Qualif Region

    Args:
        df: dataframe

    Returns:
        Dataframe with Qualif region
    """

    def find_region(text, LIST_REGIONS, regex):
        """
        Find region in text
        If multiple regions in text, return ""
        else return region

        Args:
            text: text
            LIST_REGIONS: list of regions
            regex: regex to find regions

        Returns:
            Region
        """
        # find only one region
        regions = re.findall(regex, text, re.IGNORECASE)

        # if only one region
        if len(regions) >= 1:
            return regions[0]
        else:
            return None

    # get regions
    dict_regions = get_regions_geojson()

    # put keys in list
    LIST_REGIONS = list(dict_regions.keys())

    # regex
    regex = r"\b(?:" + "|".join(map(re.escape, LIST_REGIONS)) + r")\b"

    df.loc[:, "qualif_region"] = df["text_translate"].apply(
        lambda x: find_region(x, LIST_REGIONS, regex)
    )

    return df


@flow(
    name="DLK SubFlow Qualif Incident Railway",
    flow_run_name="dlk-subflow-qualif-incident-railway",
    log_prints=True,
)
def qualif_incident_railway():
    """
    Qualif Incident Railway
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
        msg_th.terms_found_sabotage,
        mqir.qualif_region,
        mqir.qualif_dmg_eqp,
        mqir.qualif_inc_type,
        mqir.qualif_coll_with,
        mqir.qualif_prtsn_grp,
        mqir.qualif_prtsn_arr,
        mqir.qualif_prtsn_names,
        mqir.qualif_prtsn_age,
        mqir.qualif_app_laws
    FROM main.messages as msg
    LEFT JOIN main.messages_theme as msg_th USING (id_message)
    LEFT JOIN main.messages_qualif_inc_railway as mqir USING (id_message)
    WHERE msg_th.theme_inc_railway IS TRUE
    AND mqir.is_classify IS NULL
    AND mqir.idx IS NULL
    AND msg.text_translate IS NOT NULL
    ORDER BY msg.date_message ASC
    LIMIT 400;
    """

    df = get_data_into_duckdb(query)

    if df.empty:
        print("No data to classify")
        return

    data_art = []
    data_art.append({"key": "Data to Qualify", "value": df.shape[0]})

    # qualif Region
    df = qualif_region(df)
    nb_region = df[df["qualif_region"].notnull()].shape[0]

    # qualif Applied Laws
    df = qualif_app_laws(df)
    nb_app_laws = df[df["qualif_app_laws"].notnull()].shape[0]

    question_jev = {
        "incident_type": {
            "type": "choice",
            "instructions": "Which option best fits the incident described?",
            "criteria": {
                "Derailment": "Unintentional off-tracking or departure of a train from the rails",
                "Sabotage": "Deliberate act of vandalism, tampering, or destruction aimed at disrupting rail operations",
                "Fire": "Accidental or arson-related combustion involving rolling stock or railway facilities",
                "Collision": "Impact between a train and another vehicle, object, or train on the tracks",
                "Attack": "Hostile targeted violence or assault against railway assets, staff, or passengers",
                "Other": "Incidents that do not fit into any of the predefined categories",
                "None": "No incident type can be determined from the information provided",
            },
        },
        "dmg_equipment": {
            "type": "choice",
            "instructions": "Which equipment or infrastructure was damaged?",
            "criteria": {
                "Freight Train": "Damage to rolling stock used for commercial cargo transport",
                "Passengers Train": "Damage to passenger-carrying train cars or EMU/DMU sets",
                "Locomotive": "Damage specifically to traction engines or power units",
                "Relay Cabin": "Damage to signaling control rooms, interlocking cabins, or signaling huts",
                "Infrastructure": "Damage to general railway structures, bridges, or overhead catenary power lines",
                "Railroad Tracks": "Physical damage or disruption to rails, ties, ballast, or switches",
                "Electric Box": "Damage to lineside electrical cabinets, transformers, or power supply units",
                "Unknown": "Equipment damaged is unknown or unspecified in the source report",
                "None": "No equipment or infrastructure damage can be determined from the information provided",
            },
        },
        "collision_with": {
            "type": "choice",
            "instructions": "What or who was involved in the collision with the train?",
            "criteria": {
                "Human": "Collision involving a pedestrian, trespasser, or worker on or near the tracks",
                "Train": "Impact with another train, locomotive, or rail vehicle",
                "Car": "Collision involving a passenger car, SUV, or light motor vehicle (e.g., at a level crossing)",
                "Truck": "Collision with a heavy commercial vehicle, lorry, or freight truck",
                "Object": "Impact with inanimate obstacles, debris, fallen trees, animals, or equipment left on the track",
                "None": "No collision or impact can be determined from the information provided",
            },
        },
        "partisans_arrest": {
            "type": "noul",
            "instructions": "Was there an arrest of partisans or individuals involved in the incident?",
            "criteria": {
                "true": "There is clear evidence or reporting of arrests made in connection with the incident",
                "false": "No arrests were reported or can be inferred from the information provided",
            },
        },
    }

    # Correspondence between Jev IA keys and DataFrame columns
    dict_correspond = [
        {"key": "incident_type", "value": "qualif_inc_type"},
        {"key": "dmg_equipment", "value": "qualif_dmg_eqp"},
        {"key": "collision_with", "value": "qualif_coll_with"},
        {"key": "partisans_arrest", "value": "qualif_prtsn_arr"},
    ]

    # Qualif with Jev IA
    df = qualif_with_jev_ia(df, question_jev, dict_correspond)

    # Qualif with Ollama IA
    df = qualif_with_ollama_ia(df)

    # insert in DuckDB with results
    insert_query = f"""
    INSERT OR IGNORE INTO main.messages_qualif_inc_railway (
        id_message,
        qualif_region,
        qualif_dmg_eqp,
        qualif_inc_type,
        qualif_coll_with,
        qualif_prtsn_grp,
        qualif_prtsn_arr,
        qualif_prtsn_names,
        qualif_prtsn_age,
        qualif_app_laws,
        is_classify
    )
    SELECT
        id_message,
        qualif_region,
        qualif_dmg_eqp,
        qualif_inc_type,
        qualif_coll_with,
        qualif_prtsn_grp,
        qualif_prtsn_arr,
        qualif_prtsn_names,
        qualif_prtsn_age,
        qualif_app_laws,
        FALSE AS is_classify
    FROM df
    RETURNING 1
    """

    len_inserted = insert_data_into_duckdb(df, insert_query)

    data_art.append({"key": "Data Region Found", "value": nb_region})
    data_art.append({"key": "Data Applied Laws Found", "value": nb_app_laws})
    data_art.append(
        {"key": "Data Inserted messages_qualif_inc_railway", "value": len_inserted}
    )

    create_artifact("dlk-subflow-qualif-incident-railway-art", "table", data_art)


@flow(
    name="DLK SubFlow Qualif",
    flow_run_name="dlk-subflow-qualif",
    log_prints=True,
)
def subflow_datalake_qualif():
    """ """

    # Incidents Railway
    qualif_incident_railway()
