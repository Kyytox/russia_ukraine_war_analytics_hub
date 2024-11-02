import asyncio

# Functions
from extract.telegram_extract import telegram_extract
from cleaning.telegram_cleaning import telegram_cleaning


def main():
    # telegram_extract()
    telegram_cleaning()


if __name__ == "__main__":
    main()
