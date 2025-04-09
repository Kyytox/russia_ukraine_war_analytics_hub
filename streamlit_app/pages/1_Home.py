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


st.title("Russia - Ukraine War Analitycs Hub 🇷🇺🇺🇦")


# Brief introduction
st.markdown(
    """
Welcome to the Ukraine-Russia Conflict Data Hub. This platform aims to aggregate and analyze data related to the ongoing invasion of Ukraine by Russia.
"""
)

# Objectives
st.header("Project Objectives")
st.markdown(
    """
- **Data Accessibility:** Provide access to a wide range of data related to the conflict, ensuring it is easily accessible to researchers, policymakers, and the public.
- **Data Summarization:** Summarize complex datasets through intuitive graphs and visualizations to make the information more digestible and understandable.
"""
)


# Additional Information
st.header("About the Project")
st.markdown(
    """
This project is an individual effort led by a data engineer. I am committed to maintaining the accuracy and integrity of the data while ensuring it is presented in a clear manner.
"""
)

st.divider()
jump_lines(2)


dict_analytics = {
    "Incidents Russian Railways": {
        "title": "Incidents Russian Railways",
        "text": "Explore incidents related to Russian Railways since 2022.",
        "image": "streamlit_app/utils/images/inc_rail.jpeg",
        "width": 300,
        "url": "pages/2_Incidents_Russian_Railways.py",
        "tags": ["Analytics", "Graphs", "Data Sources"],
        "color_tags": ["#961010", "#4abb15", "#14aca4"],
    },
    "Blocked Web Sites in Russia": {
        "title": "Blocked Web Sites in Russia",
        "text": "Explore the list of websites blocked in Russia.",
        "image": "streamlit_app/utils/images/logo_blocked_sites.png",
        "width": 300,
        "url": "pages/3_Blocked_Websites_in_Russia.py",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
    },
    "Raid Alerts in Ukraine": {
        "title": "Raid Alerts in Ukraine",
        "text": "Explore the alerts related to missile raids in Ukraine.",
        "image": "streamlit_app/utils/images/raid_alerts_ukraine.png",
        "width": 300,
        "url": "pages/4_Raid_Alerts_Ukraine.py",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
    },
    "Components Weapons": {
        "title": "Components Weapons",
        "text": "Explore the components used in weapons.",
        "image": "streamlit_app/utils/images/components_weapons.jpg",
        "width": 300,
        "url": "pages/5_Components_Weapons.py",
        "tags": ["Analytics", "Graphs", "Data Sources"],
        "color_tags": ["#961010", "#4abb15", "#14aca4"],
    },
}

display_container_content(dict_analytics)
