import ollama

# Variables
from core.utils.variables import (
    ia_translate,
    ia_classify,
)

# Functions
from core.libs.utils import (
    format_clean_text,
)


def chat_ia(text, model_ia):
    """
    Chat with IA

    Args:
        text: text to chat
        model_ia: model IA

    Returns:
        Response of IA
    """
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
        response = chat_ia(text, ia_translate)

    return response


def format_response_classify(response):
    """
    Format response of classify

    Args:
        response: response of classify

    Returns:
        Formatted response
    """
    if "yes" in response.lower():
        return "yes"
    elif "no" in response.lower():
        return "no"

    return response


def ia_treat_message(text, mode):
    """
    Apply IA to text

    Args:
        text: text to treat
        mode: mode to apply

    Returns:
        Response of IA
    """
    if mode == "translate":
        response = chat_ia(text, ia_translate)
        response = format_response_translate(text, response)
        response = format_clean_text(response)
    elif mode == "classify":
        response = chat_ia(text, ia_classify)
        response = format_response_classify(response)

    return response
