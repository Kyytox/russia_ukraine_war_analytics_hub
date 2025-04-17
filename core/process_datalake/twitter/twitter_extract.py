import asyncio
import yaml
import pandas as pd
import os
import datetime
from twikit import Client

from prefect import flow, task


# Variables
from core.config.paths import PATH_TWITTER_RAW, PATH_CREDS_API

# Functions
from core.libs.utils import (
    read_data,
    save_data,
    concat_old_new_df,
    upd_data_artifact,
    create_artifact,
)


@task(name="Get credentials", task_run_name="get-credentials")
def get_credentials():
    """
    Get credentials

    Returns:
        list_creds: list of credentials
    """
    # get credentials
    with open(PATH_CREDS_API) as file:
        credentials = yaml.safe_load(file)

    # get all 'name'
    list_creds = []
    for account in credentials["twitter"]:
        list_creds.append(account)

    return list_creds


@task(name="Create search query", task_run_name="create-search-query")
def create_search_query(
    list_words_filter, list_accounts_filter, date_since, date_until
):
    """
    Create search query

    Args:
        list_words_filter: list of words
        list_accounts_filter: list of accounts
        date_since: date since
        date_until: date until

    Returns:
        query: str
    """

    text_words = " OR ".join(list_words_filter)
    text_accounts = " OR ".join([f"from:{account}" for account in list_accounts_filter])

    query = f"({text_words}) ({text_accounts}) until:{date_until} since:{date_since} -filter:replies"

    return query


@task(name="Get date since and until", task_run_name="get-date-since-until")
def get_date_since_until(df):
    """
    Get date since and until

    Args:
        df: dataframe

    Returns:
        date_since: str
        date_until: str
    """

    day_delta = 100

    if df.empty:
        date_since = "2022-01-01"

        # date until datesince + 4 months
        date_until = (
            datetime.datetime.strptime(date_since, "%Y-%m-%d")
            + datetime.timedelta(days=day_delta)
        ).strftime("%Y-%m-%d")
    else:
        date_since = (
            datetime.datetime.strptime(
                df.loc[df["id_message"].idxmax(), "date"], "%a %b %d %H:%M:%S %z %Y"
            )
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

        # date until datesince + 4 months
        date_until = (
            datetime.datetime.strptime(date_since, "%Y-%m-%d")
            + datetime.timedelta(days=day_delta)
        ).strftime("%Y-%m-%d")

    print("date_since from df:", date_since)
    print("date_until from df:", date_until)

    return date_since, date_until


@task(name="Search messages", task_run_name="search-messages")
async def search_messages(df_raw):
    """
    Search messages in Twitter
    With specific keywords and accounts

    Args:
        client: TwitterClient

    Returns:
        df: dataframe with messages
    """

    # init data
    list_words_filter = [
        "train",
        "derail",
        "derailment",
        "freight",
        "derailed",
        "locomotive",
        "rails",
        "carriages",
    ]
    list_accounts_filter = [
        "Prune602",
        "LXSummer1",
        "igorsushko",
        "wartranslated",
        "Schizointel",
    ]

    # init data
    df = pd.DataFrame()

    # get date since and until
    date_since, date_until = get_date_since_until(df_raw)

    # get credentials
    list_credentials = get_credentials()

    for account in list_credentials:
        data = []
        cpt_try = 0

        try:
            # Connect to Twitter
            client = Client("en-US")
            await client.login(
                auth_info_1=account["username"],
                auth_info_2=account["email"],
                password=account["password"],
            )
        except Exception as e:
            print(e)
            continue

        # get last date for other accounts
        if not df.empty:
            date_since, date_until = get_date_since_until(df)

        # create search query
        search_query = create_search_query(
            list_words_filter, list_accounts_filter, date_since, date_until
        )

        # get tweets
        list_tweets = await client.search_tweet(search_query, "Latest")

        while len(list_tweets) > 0:
            for tweet in list_tweets:

                # check if account in list_accounts_filter
                # because Twitter search is not working properly
                if tweet.user.screen_name in list_accounts_filter:
                    data.append(
                        {
                            "ID": f"{tweet.user.screen_name}_{tweet.id}",
                            "date": tweet.created_at,
                            "account": tweet.user.screen_name,
                            "id_message": tweet.id,
                            "text_original": tweet.full_text,
                        }
                    )

            # break while if more than 150 messages (limit)
            print(f"Extracted {len(data)} messages")
            if len(data) > 149:
                break

            try:
                # get next tweets
                print("Next tweets")
                list_tweets = await list_tweets.next()
                if len(list_tweets) == 0:
                    if cpt_try < 2:
                        # get date since and until of data
                        date_since, date_until = get_date_since_until(
                            pd.DataFrame(data)
                        )

                        # get next tweets
                        search_query = create_search_query(
                            list_words_filter,
                            list_accounts_filter,
                            date_since,
                            date_until,
                        )
                        list_tweets = await client.search_tweet(search_query, "Latest")

                        # increment cpt_try
                        cpt_try += 1
                    else:
                        break
            except Exception as e:
                print(e)
                break  # break while

        # add to df
        df = (
            pd.concat([df, pd.DataFrame(data)])
            .reset_index(drop=True)
            .sort_values("id_message")
        )

        if df.shape[0] == 0:
            break

    print(f"Extracted {df.shape[0]} messages")

    # add url col
    df["url"] = df.apply(
        lambda x: f"https://x.com/{x['account']}/status/{x['id_message']}", axis=1
    )

    # add col filter_theme
    df["filter_theme"] = "incident_railway"

    return df


@flow(
    name="Flow Master Twitter Extract",
    flow_run_name="flow-master-twitter-extract",
    log_prints=True,
)
def flow_twitter_extract():
    """
    Job Twitter extract
    """
    print("********************************")
    print("Start extracting Twitter")
    print("********************************")

    # get messages already extracted
    df_raw = read_data(PATH_TWITTER_RAW, "twitter")

    # get messages
    df = asyncio.run(search_messages(df_raw))

    # update data artifact
    upd_data_artifact("twitter-extract", df.shape[0])

    # concat data
    df = concat_old_new_df(df_raw, df, cols=["ID"])

    # save data
    save_data(PATH_TWITTER_RAW, "twitter", df)

    # create artifacts
    create_artifact("flow-master-twitter-extract-artifact")
