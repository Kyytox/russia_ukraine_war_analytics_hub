import os
import sys
import time

from prefect import flow

# Process Telegram
from core.process_social_network.extract.telegram_extract import job_telegram_extract
from core.process_social_network.cleaning.telegram_cleaning import job_telegram_cleaning
from core.process_social_network.transform.telegram_transform import (
    job_telegram_transform,
)
from core.process_social_network.filter.telegram_filter import job_telegram_filter

# Process Twitter
from core.process_social_network.extract.twitter_extract import job_twitter_extract
from core.process_social_network.cleaning.twitter_cleaning import job_twitter_cleaning
from core.process_social_network.filter.twitter_filter import job_twitter_filter

# Filter to classify
from core.process_social_network.classify.filter_to_classify import (
    job_filter_to_classify,
)

# Process Applicatifs
from core.process_applicatifs.extract.extract_incidents_railway import (
    job_extract_incident_railway,
)
from core.process_applicatifs.transform.transform_incidents_railway import (
    job_transform_incident_railway,
)
from core.process_applicatifs.datamarts.datamarts_incidents_railway import (
    job_datamarts_incidents_railway,
)


def process_telegram():
    """
    Process Telegram data
    """
    # job_telegram_extract()

    # job_telegram_cleaning()

    job_telegram_transform()

    # job_telegram_filter()


@flow(name="PROCESS TWITTER")
def process_twitter():
    """
    Process Twitter data
    """
    # job_twitter_extract()

    # job_twitter_cleaning()

    job_twitter_filter()


def process_applicatifs():
    """
    Process Applicatifs data
    """

    # job_extract_incident_railway()

    # job_transform_incident_railway()

    job_datamarts_incidents_railway()


def main():

    if len(sys.argv) == 1:
        print("No parameters")
        print("List of parameters:")
        print("tg")
        print("tw")

    if len(sys.argv) > 1:
        # get parameters stdout
        param = sys.argv[1]

        if param == "tg":
            process_telegram()
        elif param == "tw":
            process_twitter()
        elif param == "app":
            process_applicatifs()
        else:
            print("Invalid parameter")
            print("List of parameters:")
            print("tg")
            print("tw")

    # job_filter_to_classify()


if __name__ == "__main__":
    main()
