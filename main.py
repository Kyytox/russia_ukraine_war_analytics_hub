import sys


# Process Telegram
from core.process_datalake.telegram.telegram_extract import flow_telegram_extract
from core.process_datalake.telegram.telegram_cleaning import flow_telegram_cleaning
from core.process_datalake.telegram.telegram_transform import (
    flow_telegram_transform,
)
from core.process_datalake.telegram.telegram_filter import flow_telegram_filter

# Process Twitter
from core.process_datalake.twitter.twitter_extract import flow_twitter_extract
from core.process_datalake.twitter.twitter_cleaning import flow_twitter_cleaning
from core.process_datalake.twitter.twitter_filter import flow_twitter_filter

# Filter to classify
from core.process_datalake.filter.datalake_filter import (
    flow_datalake_filter,
)
from core.process_datalake.qualification.datalake_qualif import (
    flow_datalake_qualif,
)
from core.process_datalake.classify.classify_to_cloud import (
    flow_classify_to_cloud,
)

# Classify to Excel Final
from core.process_datalake.excel_final.classify_to_excel_final import (
    flow_classify_to_excel_final,
)

# # Process Applicatifs
from core.process_data_warehouse.flow_dwh_inc_railway import flow_dwh_inc_railway


def process_telegram():
    """
    Process Telegram data
    """
    # flow_telegram_extract()

    # flow_telegram_cleaning()

    flow_telegram_transform()

    # flow_telegram_filter()


def process_twitter():
    """
    Process Twitter data
    """
    flow_twitter_extract()

    flow_twitter_cleaning()

    # flow_twitter_filter()


def process_dwh():
    """
    Process Data Warehouse
    """

    flow_dwh_inc_railway()


def main():

    if len(sys.argv) == 1:
        print("No parameters")
        print("List of parameters:")
        print("tg")
        print("tw")
        print("filt")
        print("qual")
        print("class")
        print("app")

    if len(sys.argv) > 1:
        # get parameters stdout
        params = sys.argv[1:]

        for param in params:
            if param == "tg":
                process_telegram()
            elif param == "tw":
                process_twitter()
            elif param == "filt":
                flow_datalake_filter()
            elif param == "qual":
                flow_datalake_qualif()
            elif param == "class":
                flow_classify_to_cloud()
            elif param == "excel":
                flow_classify_to_excel_final()
            elif param == "dwh":
                process_dwh()
            else:
                print("Invalid parameter")
                print("List of parameters:")
                print("tg")
                print("tw")


if __name__ == "__main__":
    main()
