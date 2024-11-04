import time

# Functions
from process.extract.telegram_extract import telegram_extract
from process.cleaning.telegram_cleaning import telegram_cleaning
from process.transform.telegram_transform import telegram_transform
from process.filter.job_filter import job_filter
from process.classify.job_filter_to_classify import job_filter_to_classify

# variables
from utils.variables import cpt_loop_process_translate


def main():
    # telegram_extract()
    # telegram_cleaning()

    # loop over accounts
    # for i in range(cpt_loop_process_translate):
    #     print(f"Loop {i + 1}/{cpt_loop_process_translate}")
    #     telegram_transform()
    #     time.sleep(200)
    telegram_transform()

    # job_filter()

    # job_filter_to_classify()


if __name__ == "__main__":
    main()
