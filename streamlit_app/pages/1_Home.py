import streamlit as st
import pandas as pd

from streamlit_extras.card import card


st.title("Russia - Ukraine War Analitycs Hub 🇷🇺🇺🇦")


# Brief introduction
st.markdown(
    """
Welcome to the Ukraine-Russia Conflict Data Hub. This platform aims to aggregate and analyze data related to the ongoing conflict between Ukraine and Russia. Our goal is to provide insights, track developments, and support informed decision-making through data-driven analysis.
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
This project is an individual effort led by a data engineer. I am committed to maintaining the accuracy and integrity of the data while ensuring it is presented in a clear and unbiased manner.
"""
)


# Contact Information
st.header("Contact Us")
st.markdown(
    """
If you have any questions, suggestions, or would like to contribute to the project, please feel free to reach out to us at [contact@example.com](mailto:contact@example.com).
"""
)


col1, col2 = st.columns(2)

with col1:
    card(
        title="Incidents Russian Railways",
        text="Explore incidents related to Russian Railways between 2022 and 2024.",
        # image="./utils/images/inc_rail.jpeg",
        image="https://assets.bwbx.io/images/users/iqjWHBFdfxIU/i3RNq1iGQFBU/v1/-1x-1.webp",
        url="http://localhost:8501/Incidents_Russian_Railways",
        styles={
            "card": {
                "width": "500px",
                "height": "450px",
                "box-shadow": "6px 6px 10px rgba(0,0,0,0.5)",
            },
        },
        key="card1",
    )

with col2:
    card(
        title="Blocked Sites in Russia",
        text="Explore the list of websites blocked in Russia.",
        image="https://sflc.in/wp-content/uploads/2016/12/internet-censorship.png",
        url="http://localhost:8501/Blocked_Sites_in_Russia",
        styles={
            "card": {
                "width": "500px",
                "height": "450px",
                "box-shadow": "6px 6px 10px rgba(0,0,0,0.5)",
            },
        },
        key="card2",
    )
