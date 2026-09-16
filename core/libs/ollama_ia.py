import ollama

import json

# Variables
from core.config.variables import (
    IA_TRANSLATE,
    IA_TRANS_MILITARY_UNIT,
    IA_CLASSIFY,
    IA_CLASSIFY_ALL,
    IA_ASSISTANT,
)

# Functions
from core.libs.utils import (
    format_clean_text,
)


def chat_ia(text, model_ia, prompt=None):
    """
    Chat with IA

    Args:
        text: text to chat
        model_ia: model IA

    Returns:
        Response of IA
    """

    if prompt:
        text = f"{prompt} {text}"

    try:
        # translate text
        response = ollama.chat(
            model=model_ia,
            messages=[
                {"role": "user", "content": text},
            ],
        )

        return response["message"]["content"]

    except Exception as e:
        print(f"Error: {e}")
        if "111" in str(e):
            print("Ollama Not Active")
            exit()

    return ""


def format_response_translate(text, response):
    """
    Format response of translate

    Args:
        response: response of translate

    Returns:
        Formatted response
    """

    cpt_words_orginal = len(text.split())
    cpt_words_translate = len(response.split())

    # check if translation is > 60% of original
    if cpt_words_translate < cpt_words_orginal * 0.6:
        response = chat_ia(text, IA_TRANSLATE, None)

    return response


def format_response_filter(response):
    """
    Format response of filter for theme

    Args:
        response: response of classify

    Returns:
        Formatted response
    """

    if "yes" in response.lower():
        return "yes"
    elif "no" in response.lower():
        return "no"
    else:
        return "no"

    #  jump line replace
    response = (
        response.replace(", ", ",")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace(".", "")
    )

    return response


def format_response_classify(response):
    """
    Format response of classify

    Args:
        response: response of classify

    Returns:
        Formatted response
    """

    # remove string not in json format
    response = response.replace("```json", "").replace("```", "")

    # check if response is a valid json
    try:
        response = json.loads(response)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Erreur : {e}")

        return {
            "incident_type": "Other",
            "damaged_equipment": "Unknown",
            "partisans_names": None,
            "partisans_ages": None,
        }

    # check if response is a dict
    if type(response) != dict:
        print(f"Error: response is not a valid json: {response}")
        return {
            "incident_type": "Other",
            "damaged_equipment": "Unknown",
            "partisans_names": None,
            "partisans_ages": None,
        }

    # check if all id are in json
    if "incident_type" not in response:
        response["incident_type"] = "Other"
    if "damaged_equipment" not in response:
        response["damaged_equipment"] = "Unknown"
    if "partisans_names" not in response:
        response["partisans_names"] = None
    if "partisans_ages" not in response:
        response["partisans_ages"] = None

    return response


def ia_treat_message(text, mode, prompt=None):
    """
    Apply IA to text

    Args:
        text: text to treat
        mode: mode to apply

    Returns:
        Response of IA
    """
    response = None

    if mode == "translate":
        response = chat_ia(text, IA_TRANSLATE, prompt)
        response = format_response_translate(text, response)
        response = format_clean_text(response)
    elif mode == "pre_classify":
        response = chat_ia(text, IA_CLASSIFY_ALL, prompt)
        response = format_response_classify(response)
    elif mode == "filter":
        response = chat_ia(text, IA_CLASSIFY, prompt)
        response = format_response_filter(response)
        # print(f"text: {text}")
        # print(f"Response: {response}")
        # print("--------------------------------------------")
        # print("--------------------------------------------")
    elif mode == "ru_officers_kiu_translate":
        response = chat_ia(text, IA_TRANS_MILITARY_UNIT, prompt)
        response = format_clean_text(response)
    elif mode == "ru_officers_kiu_military_unit":
        response = chat_ia(text, IA_ASSISTANT, prompt)
        response = format_clean_text(response)

    return response
