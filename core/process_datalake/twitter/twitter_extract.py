import time
import asyncio
import yaml
import pandas as pd
import datetime
from typing import List, Dict, Tuple, Any, Optional
from twikit import Client
from prefect.cache_policies import TASK_SOURCE

from prefect import flow, task


# Variables
from core.config.paths import PATH_TWITTER_RAW, PATH_CREDS_API

from core.libs.utils import (
    read_data,
    save_data,
    concat_old_new_df,
    upd_data_artifact,
    create_artifact,
)


RAILWAY_WORDS = [
    "train",
    "derail",
    "derailment",
    "freight",
    "derailed",
    "locomotive",
    "rails",
    "carriages",
]

RAILWAY_ACCOUNTS = [
    "Prune602",
    "LXSummer1",
    "igorsushko",
    "wartranslated",
    "Schizointel",
]

MAX_TWEETS = 149


@task(name="Get credentials", task_run_name="get-credentials")
def get_credentials() -> List[Dict[str, str]]:
    """
    Get Twitter account credentials from config file

    Returns:
        list_creds: List of credential dictionaries containing username, email, and password
    """
    with open(PATH_CREDS_API) as file:
        credentials = yaml.safe_load(file)

    return credentials.get("twitter", [])


@task(name="Get date since and until", task_run_name="get-date-since-until")
def get_date_since_until(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Calculate date range for Twitter search query

    Args:
        df: DataFrame with historical tweet data

    Returns:
        tuple: (date_since, date_until) formatted as YYYY-MM-DD strings
    """
    # Always search up to tomorrow
    date_until = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    # Set default start date if no existing data
    if df.empty:
        date_since = "2022-01-01"
    else:
        # Get date of most recent tweet and go back one day for overlap
        latest_date = df.loc[df["id_message"].idxmax(), "date"]
        date_since = (
            datetime.datetime.strptime(latest_date, "%a %b %d %H:%M:%S %z %Y")
            - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    print(f"Search date range: {date_since} to {date_until}")

    return date_since, date_until


@task(name="Create search query", task_run_name="create-search-query")
def create_search_query(date_since: str, date_until: str) -> str:
    """
    Create Twitter search query with railway-related keywords and specific accounts

    Args:
        date_since: Start date in YYYY-MM-DD format
        date_until: End date in YYYY-MM-DD format

    Returns:
        query: Formatted Twitter search query string
    """
    text_words = " OR ".join(RAILWAY_WORDS)
    text_accounts = " OR ".join([f"from:{account}" for account in RAILWAY_ACCOUNTS])

    query = f"({text_words}) ({text_accounts}) until:{date_until} since:{date_since} -filter:replies"

    return query


@task(
    name="Get Tweets",
    task_run_name="get-tweets",
    cache_policy=TASK_SOURCE,
    persist_result=False,
)
async def get_tweets(client: Client, date_since: str, date_until: str) -> Any:
    """
    Fetch tweets using the Twitter API client with date filters

    Args:
        client: Authenticated Twitter API client
        date_since: Start date in YYYY-MM-DD format
        date_until: End date in YYYY-MM-DD format

    Returns:
        list_tweets: List of tweet objects from Twitter API
    """
    # Create search query based on keywords, accounts and dates
    search_query = create_search_query(date_since, date_until)

    # Execute search and get tweet results
    list_tweets = await client.search_tweet(search_query, "Latest")

    print(f"Retrieved {len(list_tweets) if list_tweets else 0} tweets")
    return list_tweets


@task(name="Search messages", task_run_name="search-messages")
async def search_messages(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Search Twitter for railway incident messages using multiple accounts

    Args:
        df_raw: DataFrame containing existing tweets

    Returns:
        result_df: DataFrame with newly collected tweets
    """
    last_id = 0
    result_df = pd.DataFrame()

    # Get initial date range based on existing data
    date_since, date_until = get_date_since_until(df_raw)

    # Get Twitter account credentials
    list_credentials = get_credentials()

    for account in list_credentials:
        data = []
        retry_count = 0

        # Connect to Twitter with current account
        try:
            print(f"Connecting to Twitter with {account['username']}")
            client = Client("en-US")
            await client.login(
                auth_info_1=account["username"],
                auth_info_2=account["email"],
                password=account["password"],
                cookies_file=account["cookies_file"],
            )
        except Exception as e:
            print(f"Error connecting to Twitter with {account['username']}: {e}")
            continue  # Try next account if this one fails

        # Update date range if we already have tweets from other accounts
        if not result_df.empty:
            date_since, date_until = get_date_since_until(result_df)

        # Get initial tweets
        try:
            list_tweets = await get_tweets(client, date_since, date_until)
            if not list_tweets:
                print(f"No tweets found for {account['username']}")
                continue
        except Exception as e:
            print(f"Error getting tweets with {account['username']}: {e}")
            continue

        # Process tweet batches
        while list_tweets and len(list_tweets) > 0:

            if list_tweets[0].id == last_id:
                print("No new tweets found, stopping retry")
                break

            for tweet in list_tweets:
                # Filter by account (Twitter search can include other accounts)
                if tweet.user.screen_name in RAILWAY_ACCOUNTS:
                    data.append(
                        {
                            "ID": f"{tweet.user.screen_name}_{tweet.id}",
                            "date": tweet.created_at,
                            "account": tweet.user.screen_name,
                            "id_message": tweet.id,
                            "text_original": tweet.full_text,
                        }
                    )

            print(f"Collected {len(data)} tweets so far")

            # Check if we have reached the maximum number of tweets
            if len(data) >= MAX_TWEETS:
                print(f"Reached maximum tweets limit ({MAX_TWEETS})")
                break

            last_id = list_tweets[0].id

            # Get next batch of tweets
            try:
                list_tweets = await list_tweets.next()
                # If no more tweets and we haven't reached max retries
                if not list_tweets or len(list_tweets) == 0:
                    if retry_count < 2:
                        # Try with updated date range
                        if data:
                            temp_df = pd.DataFrame(data)

                            # Get new date range based on collected tweets
                            date_since, date_until = get_date_since_until(temp_df)

                            # Retry fetching tweets with updated date range
                            list_tweets = await get_tweets(
                                client, date_since, date_until
                            )

                            retry_count += 1
                        else:
                            break
                    else:
                        break
            except Exception as e:
                print(f"Error fetching next batch of tweets: {e}")
                break

        # Add collected tweets to result DataFrame
        if data:
            new_data_df = pd.DataFrame(data)
            result_df = (
                pd.concat([result_df, new_data_df])
                .reset_index(drop=True)
                .sort_values("id_message")
            )

    # End processing if no tweets were found
    if result_df.empty:
        print("No tweets found matching criteria")
        return result_df

    print(f"Total extracted tweets: {result_df.shape[0]}")

    # Add URL and theme columns
    result_df["url"] = result_df.apply(
        lambda x: f"https://x.com/{x['account']}/status/{x['id_message']}", axis=1
    )
    result_df["filter_theme"] = "incident_railway"

    return result_df


@flow(
    name="Flow Master Twitter Extract",
    flow_run_name="flow-master-twitter-extract",
    log_prints=True,
)
def flow_twitter_extract() -> None:
    """
    Main flow function to extract railway incident tweets,
    save them to storage, and track as artifacts
    """
    print("=" * 50)
    print("Starting Twitter railway incident extraction")
    print("=" * 50)

    # Load existing data
    df_raw = read_data(PATH_TWITTER_RAW, "twitter")
    print(f"Loaded existing data: {df_raw.shape[0] if not df_raw.empty else 0} records")

    # Search for new tweets
    print("Searching for new tweets...")
    df_new = asyncio.run(search_messages(df_raw))

    if df_new.empty:
        print("No new tweets found")
        new_count = 0
    else:
        print(f"Found {df_new.shape[0]} new tweets")

        # Merge with existing data, deduplicating by ID
        df_combined = concat_old_new_df(df_raw, df_new, cols=["ID"])
        new_count = df_combined.shape[0] - (0 if df_raw.empty else df_raw.shape[0])

        print(df_combined)
        print("timestamp")
        time.sleep(120)

        # Save the combined data
        save_data(PATH_TWITTER_RAW, "twitter", df_combined)
        print(f"Saved {df_combined.shape[0]} total records (added {new_count} new)")

    # Update tracking artifacts
    upd_data_artifact("twitter-extract", new_count)
    create_artifact("flow-master-twitter-extract-artifact")

    print("Twitter extraction completed successfully")
