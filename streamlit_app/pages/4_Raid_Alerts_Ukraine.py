import streamlit as st


from utils import (
    jump_lines,
    init_css,
    add_analytics_tag,
    developper_link,
    display_container_content,
)


# Google Analytics
add_analytics_tag()

# CSS
init_css()


with st.sidebar:
    developper_link()


st.title("Raid Alerts in Ukraine 🚨")
st.divider()


dict_data = {
    "Air Raid Alert Map of Ukraine": {
        "title": "Air Raid Alert Map of Ukraine",
        "text": "Map of air raid alerts in Ukraine",
        "image": None,
        "url": "https://alerts.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
    },
    "Air-alarms.in.ua": {
        "title": "Air-alarms.in.ua",
        "text": "Statistics of air alerts in Ukraine",
        "image": None,
        "url": "https://air-alarms.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
    },
    "Ukrainian Air Raid Sirens Dataset": {
        "title": "Ukrainian Air Raid Sirens Dataset",
        "text": "GitHub repository with the Ukrainian Air Raid Sirens Dataset",
        "image": None,
        "url": "https://github.com/Vadimkin/ukrainian-air-raid-sirens-dataset",
        "tags": ["Data Sources"],
        "color_tags": ["#14aca4"],
    },
}


display_container_content(dict_data)
