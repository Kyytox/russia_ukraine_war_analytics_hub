import sys


# Process Telegram
from core.process_datalake.telegram.telegram_extract import flow_telegram_extract
from core.process_datalake.telegram.telegram_cleaning import flow_telegram_cleaning
from core.process_datalake.telegram.telegram_transform import (
    flow_telegram_transform,
)

# Process Twitter
from core.process_datalake.twitter.twitter_extract import flow_twitter_extract
from core.process_datalake.twitter.twitter_cleaning import flow_twitter_cleaning

# Filter to classify
from core.process_datalake.filter.datalake_filter import (
    flow_datalake_filter,
)
from core.process_datalake.qualification.datalake_qualif import (
    flow_datalake_qualif,
)

# Classify to Excel Final
from core.process_datalake.classify.classify_to_cloud import (
    flow_classify_to_cloud,
)
from core.process_datalake.classify.cloud_to_classify import (
    flow_cloud_to_classify,
)


# # Process Applicatifs
from core.process_data_warehouse.flow_dwh_inc_railway import flow_dwh_inc_railway
from core.process_data_warehouse.flow_dwh_ru_block_sites import flow_dwh_ru_block_sites


def process_telegram():
    """
    Process Telegram data
    """
    flow_telegram_extract()
    flow_telegram_cleaning()
    flow_telegram_transform()


def process_twitter():
    """
    Process Twitter data
    """
    flow_twitter_extract()
    flow_twitter_cleaning()


def process_dwh():
    """
    Process Data Warehouse
    """
    flow_dwh_inc_railway()
    # flow_dwh_ru_block_sites()


COMMANDS = {
    "tg": process_telegram,
    "tw": process_twitter,
    "filt": flow_datalake_filter,
    "qual": flow_datalake_qualif,
    "class": flow_classify_to_cloud,
    "sync": flow_cloud_to_classify,
    "dwh": process_dwh,
}


def print_help():
    print("No parameters or invalid parameter.")
    print("List of valid parameters:")
    for c in COMMANDS.keys():
        print(f"- {c}")


def main():
    if len(sys.argv) < 2:
        print_help()
        return

    params = sys.argv[1:]

    for param in params:
        if param in COMMANDS:
            COMMANDS[param]()
        else:
            print_help()
            return


if __name__ == "__main__":
    main()
