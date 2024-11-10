import os
import sys
import time

from prefect import flow

# Functions
from code.process.extract.telegram_extract import job_telegram_extract
from code.process.cleaning.telegram_cleaning import job_telegram_cleaning
from code.process.transform.telegram_transform import job_telegram_transform
from code.process.filter.telegram_filter import job_telegram_filter
from code.process.classify.filter_to_classify import job_filter_to_classify

# variables
from code.utils.variables import cpt_loop_process_translate


@flow()
def process_telegram():
    """
    Process Telegram data
    """
    # job_telegram_extract()

    # job_telegram_cleaning()

    # job_telegram_transform()
    # loop over accounts
    # for i in range(cpt_loop_process_translate):
    #     print(f"Loop {i + 1}/{cpt_loop_process_translate}")
    #     telegram_transform()
    #     time.sleep(150)

    # job_telegram_filter()


def main():

    if len(sys.argv) > 1:
        # get parameters stdout
        param = sys.argv[1]
        print(f"Parameter: {param}")

        if param == "telegram":
            process_telegram()
    else:
        print("No parameter")
        print("Please provide a parameter")
        print("Example: python main.py telegram")

    job_filter_to_classify()


if __name__ == "__main__":
    main()
