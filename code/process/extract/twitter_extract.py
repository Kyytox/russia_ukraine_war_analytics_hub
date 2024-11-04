import asyncio
from twikit import Client

USERNAME = "04Kyx60905"
EMAIL = "kytox.dev04@yahoo.com"
PASSWORD = "kyx.dev__04"

# Initialize client
client = Client("en-US")


async def main():
    await client.login(auth_info_1=USERNAME, auth_info_2=EMAIL, password=PASSWORD)

    # Get user by screen name
    USER_SCREEN_NAME = "Kytox_"
    user = await client.get_user_by_screen_name(USER_SCREEN_NAME)

    # Access user attributes
    print(
        f"id: {user.id}",
        f"name: {user.name}",
        f"followers: {user.followers_count}",
        f"tweets count: {user.statuses_count}",
        sep="\n",
    )

    tweets = await client.search_tweet(
        "(train OR derail OR derailment OR freight OR derailed OR locomotive OR rails OR carriages) (from:Prune602 OR from:LXSummer1 OR from:igorsushko OR from:wartranslated OR from:Schizointel) until:2024-12-31 since:2024-01-01",
        "Top",
        4,
    )

    for tweet in tweets:
        print("\n")
        print(
            f"Tweet ID: {tweet.id}",
            f"Tweet text: {tweet.text}",
            f"Tweet date: {tweet.created_at}",
            f"Tweet user: {tweet.user.screen_name}",
            sep="\n",
        )


asyncio.run(main())
