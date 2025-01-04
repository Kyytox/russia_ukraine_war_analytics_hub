import re
import pandas as pd
import numpy as np
from prefect import flow, task

# Functions
from core.libs.utils import (
    get_telegram_accounts,
    read_data,
    save_data,
)

# Variables
from core.config.paths import (
    PATH_TELEGRAM_TRANSFORM,
    PATH_TWITTER_CLEAN,
    PATH_FILTER_SOCIAL_MEDIA,
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


@task(name="Get Telegram data")
def get_telegram_data():
    """
    Get Telegram data

    Returns:
        Dataframe with Telegram data
    """

    # get list accounts
    list_accounts = get_telegram_accounts(PATH_TELEGRAM_TRANSFORM)

    # remove belzhd_live
    list_accounts.remove("belzhd_live")

    # group data
    df_list = [read_data(PATH_TELEGRAM_TRANSFORM, account) for account in list_accounts]

    # concat data
    df_telegram = pd.concat(df_list).sort_values("date").reset_index(drop=True)

    # convert to str
    df_telegram["id_message"] = df_telegram["id_message"].astype(str)

    print(f"Data Telegram Extract: {df_telegram.shape}")
    return df_telegram


@task(name="Get Twitter data")
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


@task(name="Regroup data")
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


@task(name="Apply filters {type_filter}")
def apply_filters(df, df_filter, type_filter):
    """
    Apply filter

    Args:
        df: dataframe with filter data

    Returns:
        Dataframe with filter data
    """

    # if type_filter == "railway":
    #     col_filter = "filter_inc_railway"
    #     col_found_terms = "found_terms_railway"
    #     lst_all_terms = [
    #         list_words_set_railway,
    #         list_substr_set_railway,
    #         list_expression_railways,
    #         list_word_railways,
    #     ]

    # elif type_filter == "arrest":
    #     col_filter = "filter_inc_arrest"
    #     col_found_terms = "found_terms_arrest"
    #     lst_all_terms = [
    #         list_words_set_arrest,
    #         list_substr_set_arrest,
    #         list_expression_arrest,
    #         list_word_arrest,
    #     ]
    # elif type_filter == "sabotage":
    #     col_filter = "filter_inc_sabotage"
    #     col_found_terms = "found_terms_sabotage"
    #     lst_all_terms = [
    #         list_words_set_sabotage,
    #         list_substr_set_sabotage,
    #         list_expression_sabotage,
    #         list_word_sabotage,
    #     ]

    # # init Filters list with list and id _filter
    # dict_filters = [
    #     {"list_terms": lst_all_terms[0], "id_filter": 1},
    #     {"list_terms": lst_all_terms[1], "id_filter": 2},
    #     {"list_terms": lst_all_terms[2], "id_filter": 3},
    #     {"list_terms": lst_all_terms[3], "id_filter": 3},
    # ]

    filter_config = {
        "railway": {
            "col_filter": "filter_inc_railway",
            "col_terms": "found_terms_railway",
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
            "terms_lists": [
                (list_words_set_sabotage, 1),
                (list_substr_set_sabotage, 2),
                (list_expression_sabotage, 3),
                (list_word_sabotage, 3),
            ],
        },
    }

    config = filter_config[type_filter]
    col_filter = config["col_filter"]
    col_terms = config["col_terms"]
    dict_filters = config["terms_lists"]

    if col_filter in df_filter.columns:
        print(f"{col_filter} has already been applied, so keep data not filtered")
        df_tmp = df[~df["ID"].isin(df_filter["ID"])]
    else:
        print(f"{col_filter} has not been applied yet")
        df_tmp = df.copy()

    print(f"Data to filter: {df_tmp.shape}")

    # Init cols
    df_tmp[col_filter] = df_tmp["filter_theme"] == f"incident_{type_filter}"
    df_tmp[f"found_terms_{type_filter}"] = ""
    df_tmp[f"add_final_inc_{type_filter}"] = False

    # apply filter according to terms
    for terms, id_filter in dict_filters:
        mask = (df_tmp[col_filter] == False) & pd.notna(df_tmp["text_translate"])
        print(f"Data mask to filter: {df_tmp[mask].shape}")

        results = df_tmp.loc[mask, "text_translate"].apply(
            lambda x: find_terms_in_text(
                terms,
                x,
                id_filter=id_filter,
            )
        )

        # Zip results
        filter_values, terms_values = zip(*results)

        # update values
        df_tmp.loc[mask, col_filter] = filter_values
        df_tmp.loc[mask, col_terms] = terms_values

    # Concat data
    df_final = pd.concat([df, df_tmp]).drop_duplicates(subset=["ID"], keep="last")
    print(
        f"Data filtered for incident {type_filter}: {df_final[df_final[col_filter] == True].shape}"
    )

    return df_final


@flow(name="Process Social Media Filter", log_prints=True)
def job_social_media_filter():
    """
    Process filter
    """
    # get Filter data
    df_filter = read_data(PATH_FILTER_SOCIAL_MEDIA, "filter_social_media")

    # get Telegram data
    df_telegram = get_telegram_data()

    # get Twitter data
    df_twitter = get_twitter_data()

    # group data
    df = regroup_data(df_telegram, df_twitter)

    # for test keep 5000
    # if df_filter.empty:
    #     df = df[0:1000]
    # else:
    #     df = df[0:2000]

    # Apply filters
    df = apply_filters(df, df_filter, "railway")
    df = apply_filters(df, df_filter, "arrest")
    df = apply_filters(df, df_filter, "sabotage")

    # get data Filtered, according to filter_inc
    df = df[
        df[[col for col in df.columns if "filter_inc" in col]].any(axis=1)
    ].reset_index(drop=True)

    # remove filter_theme
    df = df.drop(columns=["filter_theme"])

    # update filter data
    if not df_filter.empty:

        # get cols of filter
        cols_filter = df_filter.columns
        cols_filter = cols_filter[1:]  # remove ID

        df_tmp = df[df["ID"].isin(df_filter["ID"])].reset_index(drop=True)
        df_tmp.drop(columns=cols_filter, inplace=True)
        print(f"Data Filtered: {df_tmp.shape}")

        if len(df_tmp.columns) > 1:
            # merge data
            df_filter = pd.merge(
                df_filter,
                df_tmp,
                on="ID",
                how="left",
            )

        # concat data
        df_tmp = df[~df["ID"].isin(df_filter["ID"])].reset_index(drop=True)
        print(f"Data Not Filtered: {df_tmp.shape}")
        df_filter = pd.concat([df_filter, df_tmp]).reset_index(drop=True)

        # fillana bool cols
        for col in df_filter.columns:
            if col.startswith("filter_inc") or col.startswith("add_final"):
                df_filter[col] = df_filter[col].fillna(False)

    else:
        df_filter = df

    # save data
    print(f"Data Final: {df_filter}")
    save_data(PATH_FILTER_SOCIAL_MEDIA, "filter_social_media", df=df_filter)
