# Process
# - get list of accounts
# - collect all transform data
# - filter incidents railway
# - save data

import re
import pandas as pd

from libs.utils import get_telegram_accounts, read_data, save_data

# Variables
from utils.variables import (
    path_telegram_transform,
    path_telegram_filter,
    list_words_set_railway,
    list_substr_set_railway,
    list_en_expression_railways,
    list_en_words_railways,
)


def find_terms_in_text(list_terms, text, id_filter):
    """
    Find terms in text

    Args:
        list_terms: list of terms to find
        text: text

    Returns:
        True if term in text
    """
    # do a match case with the id_filter
    match id_filter:
        case 1:
            for word_set in list_terms:
                if all(re.search(rf"\b{word}\b", text) for word in word_set):
                    return True
        case 2:
            for word_set in list_terms:
                if all(word in text for word in word_set):
                    return True
        case 3:
            regex = r"\b(?:" + "|".join(map(re.escape, list_terms)) + r")\b"
            return bool(re.search(regex, text, re.IGNORECASE))

    return False


def apply_filter_railway(df):
    """
    Apply filter to df_filter

    Args:
        df: dataframe with filter data

    Returns:
        Dataframe with filter data
    """

    # init Filters list with list and id _filter
    dict_filters = [
        {"list_terms": list_words_set_railway, "id_filter": 1},
        {"list_terms": list_substr_set_railway, "id_filter": 2},
        {"list_terms": list_en_expression_railways, "id_filter": 3},
        {"list_terms": list_en_words_railways, "id_filter": 3},
    ]

    # add filter_railway column
    df["filter_railway"] = False

    # apply filters
    for filter in dict_filters:
        df.loc[df["filter_railway"] == False, "filter_railway"] = df[
            df["filter_railway"] == False
        ]["text_translate"].apply(
            lambda x: find_terms_in_text(
                filter["list_terms"], x, id_filter=filter["id_filter"]
            )
        )

    df = df[df["filter_railway"]].reset_index(drop=True)
    print(f"Data filtered: {df.shape}")

    # add cols
    df.loc[:, "is_incident_railway"] = True
    df.loc[:, "is_add_to_final_dataset"] = False

    return df


def filter_incidents_railway():
    """
    Filter incidents railway
    """

    # get list of accounts
    list_accounts_telegram = get_telegram_accounts(path_telegram_transform)

    # collect all transform data
    df_list = [
        read_data(path_telegram_transform, account)
        for account in list_accounts_telegram
    ]

    # concat data
    df_to_filter = pd.concat(df_list).sort_values("date").reset_index(drop=True)

    # apply filter
    df = apply_filter_railway(df_to_filter)

    # save data
    save_data(path_telegram_filter, "incidents_railway", df=df)
