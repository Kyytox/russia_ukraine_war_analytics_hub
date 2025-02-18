import hashlib
import re
import pandas as pd
from prefect import flow, task

# Functions
from core.libs.utils import (
    get_telegram_accounts,
    read_data,
    save_data,
    upd_data_artifact,
    create_artifact,
)

# Variables
from core.config.paths import (
    PATH_TELEGRAM_TRANSFORM,
    PATH_TWITTER_CLEAN,
    PATH_FILTER_DATALAKE,
)

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


def find_terms_in_text(list_terms, text, id_filter):
    """
    Find terms in text

    Args:
        list_terms: list of terms to find
        text: text

    Returns:
        Tuple (True if term in text, list of found terms)
    """
    match id_filter:
        case 1:
            # find if all words in word_set are in text
            for word_set in list_terms:
                if all(re.search(rf"\b{word}\b", text) for word in word_set):
                    return True, ",".join(word_set)
        case 2:
            # find if all words in word_set are in text
            for word_set in list_terms:
                if all(word in text for word in word_set):
                    return True, ",".join(word_set)
        case 3:
            # find if any word in word_set is in text
            found_terms = [
                term
                for term in list_terms
                if re.search(rf"\b{re.escape(term)}\b", text, re.IGNORECASE)
            ]
            return bool(found_terms), ",".join(found_terms)

    return False, None


@task(name="Get Telegram data", task_run_name="get-telegram-data")
def get_telegram_data():
    """
    Get Telegram data

    Returns:
        Dataframe with Telegram data
    """

    # read data transform
    df_telegram = read_data(PATH_TELEGRAM_TRANSFORM, "transform_telegram")

    # convert to str
    df_telegram["id_message"] = df_telegram["id_message"].astype(str)

    print(f"Data Telegram Extract: {df_telegram.shape}")
    return df_telegram


@task(name="Get Twitter data", task_run_name="get-twitter-data")
def get_twitter_data():
    """
    Get Twitter data

    Returns:
        Dataframe with Twitter data
    """

    # read data from clean
    df_twitter = read_data(PATH_TWITTER_CLEAN, "twitter")

    print(f"Data Twitter Extract: {df_twitter.shape}")
    return df_twitter


@task(name="Regroup data", task_run_name="regroup-data")
def regroup_data(df_telegram, df_twitter):
    """
    Regroup data

    Args:
        df_telegram: dataframe with Telegram data
        df_twitter: dataframe with Twitter data

    Returns:
        Dataframe with regrouped data
    """

    # group data
    df = pd.concat([df_telegram, df_twitter]).sort_values("date").reset_index(drop=True)

    # remove account, id_message
    df = df.drop(columns=["account", "id_message"])

    print(f"Total Data Regrouped: {df.shape}")
    return df


@task(name="Remove data not pertinant", task_run_name="remove-data-not-pertinant")
def remove_data_not_pertinant(df):
    """
    Remove data not pertinant

    Args:
        df: dataframe with data

    Returns:
        Dataframe with data not pertinant
    """

    list_words = [
        "COVID",
        "covid",
    ]

    # remove data containing words (except text_translate is null)
    mask = pd.notna(df["text_translate"]) & df["text_translate"].str.contains(
        "|".join(list_words)
    )

    # remove data
    df = df[~mask].reset_index(drop=True)

    print(f"Data Not Pertinant: {df.shape}")
    return df


@task(name="Generate Hash Filter", task_run_name="generate-hash-filter")
def generate_hash_filter(dict_filters):
    """
    Generate hash filter

    Args:
        dict_filters: dictionary with filters

    Returns:
        String with hash filter
    """

    # Regroupe filters
    long_text = ""

    for key, index in dict_filters:
        # convert to string
        if isinstance(key[0], tuple):
            for x in sorted(key):
                long_text += f"{''.join(x)}"
        else:
            long_text += f"{''.join(key)}"

    long_text = long_text.strip().lower()
    long_text = re.sub(r"\s+", "", long_text)  # remove spaces

    hashed_data = hashlib.md5(long_text.encode())

    return hashed_data.hexdigest()


@task(name="Apply filters {type_filter}", task_run_name="apply-filters-{type_filter}")
def apply_filters(df, type_filter, config_filter, hash_filt):
    """
    Apply filters to a DataFrame based on specified configuration and type.

    Args:
        df: DataFrame to filter
        type_filter: Type of filter to apply
        config_filter: Configuration for the filter
        hash_filt: Hash of the filter

    Returns:
        DataFrame with filter applied
    """

    # get config
    col_filter = config_filter["col_filter"]
    col_terms = config_filter["col_terms"]
    col_add_final = config_filter["col_add_final"]
    col_hash = config_filter["col_hash"]
    dict_filters = config_filter["terms_lists"]

    # Init cols
    df[col_filter] = df["filter_theme"] == f"incident_{type_filter}"
    df[col_terms] = ""
    df[col_add_final] = False

    # apply filter according to terms
    for terms, id_filter in dict_filters:

        # get
        mask = (df[col_filter] == False) & pd.notna(df["text_translate"])
        print(f"Data mask to filter: {df[mask].shape}")

        results = df.loc[mask, "text_translate"].apply(
            lambda x: find_terms_in_text(
                terms,
                x,
                id_filter=id_filter,
            )
        )

        # Zip results
        filter_values, terms_values = zip(*results)

        # update values
        df.loc[mask, col_filter] = filter_values
        df.loc[mask, col_terms] = terms_values

    # add hash filter to data
    df[col_hash] = hash_filt

    print(f"Data filtered for incident {type_filter}: {df.shape}")
    return df


@task(name="Update final data", task_run_name="update-final-data")
def update_final_data(df, df_old, type):
    """
    Update final data

    Args:
        df: dataframe with data
        df_old: dataframe with old data
        type: type of data

    Returns:
        Dataframe with updated data
    """
    if df_old.empty:
        return df

    # check if cols exist in data
    cols_to_add = [col for col in df.columns if col not in df_old.columns]
    print(f"Cols to add: {cols_to_add}")

    if cols_to_add:
        mask_exist = df[df["ID"].isin(df_old["ID"])]
        mask_exist = mask_exist[["ID"] + cols_to_add]

        df_old = pd.merge(
            df_old,
            mask_exist,
            on="ID",
            how="outer",
        )

    if type == "hash":
        mask = df
    else:
        # get data not in old
        mask = df[~df["ID"].isin(df_old["ID"])]

    df_old = (
        pd.concat([df_old, mask])
        .drop_duplicates(subset=["ID"], keep="last")
        .sort_values("date" if "date" in df_old.columns else [])
        .reset_index(drop=True)
        .ffill()
    )

    return df_old


@flow(
    name="Flow Master Datalake Filter",
    flow_run_name="flow-master-datalake-filter",
    log_prints=True,
)
def flow_datalake_filter():
    """
    Process filter
    """

    # get Filter data
    df_filter_old = read_data(PATH_FILTER_DATALAKE, "filter_datalake")

    # get Hash Filter
    df_hash_filte_old = read_data(PATH_FILTER_DATALAKE, "hash_filter_datalake")

    # get Telegram data
    df_telegram = get_telegram_data()

    # get Twitter data
    df_twitter = get_twitter_data()

    # group data
    df = regroup_data(df_telegram, df_twitter)

    # remove Data not pertinant
    df = remove_data_not_pertinant(df)

    df_filtered = pd.DataFrame()

    theme_list = ["railway", "arrest", "sabotage"]

    for theme in theme_list:

        dict_filter_config = {
            "railway": {
                "col_filter": "filter_inc_railway",
                "col_terms": "found_terms_railway",
                "col_add_final": "add_final_inc_railway",
                "col_hash": "hash_filter_railway",
                "terms_lists": [
                    (list_words_set_railway, 1),
                    (list_substr_set_railway, 2),
                    (list_expression_railways, 3),
                    (list_word_railways, 3),
                ],
            },
            "arrest": {
                "col_filter": "filter_inc_arrest",
                "col_terms": "found_terms_arrest",
                "col_add_final": "add_final_inc_arrest",
                "col_hash": "hash_filter_arrest",
                "terms_lists": [
                    (list_words_set_arrest, 1),
                    (list_substr_set_arrest, 2),
                    (list_expression_arrest, 3),
                    (list_word_arrest, 3),
                ],
            },
            "sabotage": {
                "col_filter": "filter_inc_sabotage",
                "col_terms": "found_terms_sabotage",
                "col_add_final": "add_final_inc_sabotage",
                "col_hash": "hash_filter_sabotage",
                "terms_lists": [
                    (list_words_set_sabotage, 1),
                    (list_substr_set_sabotage, 2),
                    (list_expression_sabotage, 3),
                    (list_word_sabotage, 3),
                ],
            },
        }

        config_filt = dict_filter_config[theme]

        # generate hash of filter
        hash_filt = generate_hash_filter(config_filt["terms_lists"])

        # keep data to filter
        if config_filt["col_hash"] not in df_hash_filte_old.columns:
            df_to_filter = df.copy()
        else:
            # get data with hash different
            mask = df_hash_filte_old[config_filt["col_hash"]] != hash_filt
            df_to_filter = df[
                df["ID"].isin(df_hash_filte_old[mask]["ID"])
                | ~df["ID"].isin(df_hash_filte_old["ID"])
            ]
            print(f"Data to filter: {df_to_filter.shape}")

            # remove from to_filter, data already filtered
            df_to_filter = df_to_filter[~df_to_filter["ID"].isin(df_filter_old["ID"])]
            print(f"Data to filter: {df_to_filter.shape}")

        print(f"Data to filter: {df_to_filter.shape}")

        if df_to_filter.empty:
            print("No data to filter")
            continue

        # update artifact
        upd_data_artifact(f"Data to filter for {theme}", df_to_filter.shape[0])

        # Apply filters
        if df_filtered.empty:
            df_filtered = apply_filters(df_to_filter, theme, config_filt, hash_filt)
        else:
            df_filtered = pd.merge(
                df_filtered,
                apply_filters(df_to_filter, theme, config_filt, hash_filt),
                on=[
                    "ID",
                    "date",
                    "text_original",
                    "text_translate",
                    "url",
                    "filter_theme",
                ],
                how="outer",
            )

        # update artifact
        upd_data_artifact(f"Data filtered for {theme}", df_filtered.shape[0])

    if df_filtered.empty:
        print("No data to filter")
        return

    # get hash data
    cols_hashs = ["ID"] + [col for col in df_filtered.columns if "hash_filter" in col]
    df_hashed = df_filtered[df_filtered[cols_hashs].notna().any(axis=1)][cols_hashs]
    print(f"Data df_hashed: {df_hashed}")

    # remove hash cols
    df_filtered = df_filtered.drop(columns=cols_hashs[1:])
    df_filtered = df_filtered.drop(columns=["filter_theme"])

    # get data Filtered, according to filter_inc
    df_filtered = (
        df_filtered[
            df_filtered[
                [col for col in df_filtered.columns if "filter_inc" in col]
            ].any(axis=1)
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )

    # if text_translate is empty, replace by text_original
    df_filtered["text_translate"] = df_filtered["text_translate"].fillna(
        df_filtered["text_original"]
    )

    # update hash filter
    df_hash_final = update_final_data(df_hashed, df_hash_filte_old, "hash")
    df_filter_final = update_final_data(df_filtered, df_filter_old, "filter")

    # save data
    print(f"Data Hash: {df_hash_final}")
    save_data(PATH_FILTER_DATALAKE, "hash_filter_datalake", df=df_hash_final)

    # save data
    print(f"Data Final: {df_filter_final}")
    save_data(PATH_FILTER_DATALAKE, "filter_datalake", df=df_filter_final)

    # create artifact
    create_artifact("flow-master-datalake-filter-artifact")
