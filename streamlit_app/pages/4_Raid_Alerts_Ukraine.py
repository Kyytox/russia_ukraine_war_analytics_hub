import streamlit as st


from utils import (
    jump_lines,
    init_css,
    add_analytics_tag,
    developper_link,
    display_container_content,
    tagger_component,
)


# Google Analytics
add_analytics_tag()

# CSS
init_css()


with st.sidebar:
    developper_link()


st.title("Raid Alerts in Ukraine 🚨")
st.divider()


# dict_data = {
#     "Air Raid Alert Map of Ukraine": {
#         "title": "Air Raid Alert Map of Ukraine",
#         "text": "Map of air raid alerts in Ukraine",
#         "image": None,
#         "url": "https://alerts.in.ua/en",
#         "tags": ["Analytics", "Graphs"],
#         "color_tags": ["#961010", "#4abb15"],
#         "column": 1,
#     },
#     "Air-alarms.in.ua": {
#         "title": "Air-alarms.in.ua",
#         "text": "Statistics of air alerts in Ukraine",
#         "image": None,
#         "url": "https://air-alarms.in.ua/en",
#         "tags": ["Analytics", "Graphs"],
#         "color_tags": ["#961010", "#4abb15"],
#         "column": 1,
#     },
#     "Ukrainian Air Raid Sirens Dataset": {
#         "title": "Ukrainian Air Raid Sirens Dataset",
#         "text": "GitHub repository with the Ukrainian Air Raid Sirens Dataset",
#         "image": None,
#         "url": "https://github.com/Vadimkin/ukrainian-air-raid-sirens-dataset",
#         "tags": ["Data Sources"],
#         "color_tags": ["#14aca4"],
#         "column": 2,
#     },
# }


# display_container_content(dict_data)


col1, col2 = st.columns([0.5, 0.5], gap="medium")

dict_data = {
    "Air Raid Alert Map of Ukraine": {
        "title": "Air Raid Alert Map of Ukraine",
        "text": "Map of air raid alerts in Ukraine",
        "image": None,
        "url": "https://alerts.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
        "column": col1,
    },
    "Air-alarms.in.ua": {
        "title": "Air-alarms.in.ua",
        "text": "Statistics of air alerts in Ukraine",
        "image": None,
        "url": "https://air-alarms.in.ua/en",
        "tags": ["Analytics", "Graphs"],
        "color_tags": ["#961010", "#4abb15"],
        "column": col2,
    },
    "Ukrainian Air Raid Sirens Dataset": {
        "title": "Ukrainian Air Raid Sirens Dataset",
        "text": "GitHub repository with the Ukrainian Air Raid Sirens Dataset",
        "image": None,
        "url": "https://github.com/Vadimkin/ukrainian-air-raid-sirens-dataset",
        "tags": ["Data Sources"],
        "color_tags": ["#14aca4"],
        "column": col1,
    },
}

# loop through the dictionary and add in each column
for key, value in dict_data.items():
    with value["column"]:
        with st.container(border=True):
            html_code = f"<h2 style='text-align: center;'>{value['title']}</h2>"
            st.markdown(html_code, unsafe_allow_html=True)

            html_code = f"<p style='text-align: center;'>{value['text']}</p>"
            st.markdown(html_code, unsafe_allow_html=True)

            tagger_component(
                "",
                value["tags"],
                color_name=value["color_tags"],
            )

            st.link_button(label=f"{value['title']} ->", url=value["url"])
