import re
import pandas as pd
from prefect import flow, task

from core.libs.utils import get_telegram_accounts, read_data, save_data

# Variables
from core.config.paths import (
    PATH_TELEGRAM_TRANSFORM,
    PATH_TELEGRAM_FILTER,
)

from core.utils.terms_filter.terms_incidents_railway import (
    list_words_set_railway,
    list_substr_set_railway,
    list_expression_railways,
    list_word_railways,
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


@task(name="Filter incidents railway")
def filter_incidents_railway(df):
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
        {"list_terms": list_expression_railways, "id_filter": 3},
        {"list_terms": list_word_railways, "id_filter": 3},
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

    # remove col filter_raiway
    df = df.drop(columns=["filter_railway"])

    return df


def regroup_data(list_accounts):
    """
    Regroup data

    Args:
        list_accounts: list of accounts

    Returns:
        Dataframe with regrouped data
    """

    # group data
    df_list = [read_data(PATH_TELEGRAM_TRANSFORM, account) for account in list_accounts]

    # concat data
    df_to_filter = pd.concat(df_list).sort_values("date").reset_index(drop=True)

    return df_to_filter


@flow(name="Process filter", log_prints=True)
def flow_telegram_filter():
    """
    Process filter
    """

    # get list accounts
    list_accounts = get_telegram_accounts(PATH_TELEGRAM_TRANSFORM)

    # remove belzhd_live
    list_accounts.remove("belzhd_live")

    # regroup data
    df_to_filter = regroup_data(list_accounts)

    # apply filter
    df = filter_incidents_railway(df_to_filter)

    # save data
    save_data(PATH_TELEGRAM_FILTER, "incidents_railway", df=df)
