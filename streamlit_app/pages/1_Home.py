import streamlit as st

# from streamlit_extras.card import card
from utils import jump_lines, init_css, add_analytics_tag, developper_link

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
Welcome to the Ukraine-Russia Conflict Data Hub. This platform aims to aggregate and analyze data related to the ongoing conflict between Ukraine and Russia.
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


st.markdown(
    """
    <style>
    [data-testid="stPageLink-NavLink"] {
        padding-left: 23px;
        padding-right: 23px;
        padding-bottom: 10px;
        padding-top: 10px;
        border-radius: 9px;
        background: #0057B8;
        border: none;
        font-family: inherit;
        text-align: center;
        cursor: pointer;
        transition: 0.4s;
    }

    [data-testid="stPageLink-NavLink"]:hover {
        background: #0057B8;
        box-shadow: 7px 10px 70px -14px #FFD700;
    }

    [data-testid="stPageLink-NavLink"]:active {
        transform: scale(0.97);
        box-shadow: 7px 10px 70px -10px #FFD700;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

col1, col2, col3 = st.columns([1, 0.15, 0.15])


dict_analytics = {
    "Incidents Russian Railways": {
        "title": "Incidents Russian Railways",
        "text": "Explore incidents related to Russian Railways since 2022.",
        "image": "streamlit_app/utils/images/inc_rail.jpeg",
        "url": "pages/2_Incidents_Russian_Railways.py",
    },
    "Blocked Web Sites in Russia": {
        "title": "Blocked Web Sites in Russia",
        "text": "Explore the list of websites blocked in Russia.",
        "image": "streamlit_app/utils/images/logo_blocked_sites.png",
        "url": "pages/3_Blocked_Websites_in_Russia.py",
    },
}

for key, value in dict_analytics.items():
    with col1:
        with st.container(border=True):

            subcol1, subcol2, subcol3 = st.columns(
                [0.3, 0.4, 0.3], vertical_alignment="center", gap="small"
            )

            with subcol1:
                st.image(value["image"], use_container_width=True)
            with subcol2:
                html_code = f"<h2 style='text-align: center;'>{value['title']}</h2>"
                st.markdown(html_code, unsafe_allow_html=True)

                html_code = f"<p style='text-align: center;'>{value['text']}</p>"
                st.markdown(html_code, unsafe_allow_html=True)
            with subcol3:
                st.page_link(value["url"], label=f"{value['title']} ->")

        jump_lines(2)
