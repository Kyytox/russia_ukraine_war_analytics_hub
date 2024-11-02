from prefect import flow, task


def extract_source():
    """
    Extracts the data source
    """
    print("Extracting the data source")


if __name__ == "__main__":
    extract_source()
