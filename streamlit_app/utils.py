import json
import pandas as pd


def get_region():
    """
    Get the region and id from the json file
    """

    # read file json
    with open("core/utils/ru_region.json") as file:
        data = json.load(file)

    # get id and name
    dict_region = {}
    for i in range(len(data["features"])):
        dict_region[data["features"][i]["properties"]["name"]] = data["features"][i][
            "id"
        ]

    # update dict
    dict_region = {
        k.replace("Moskva", "Moscow").replace("'", ""): v
        for k, v in dict_region.items()
    }

    return dict_region
