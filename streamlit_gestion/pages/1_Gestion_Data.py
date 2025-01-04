import os
import sys
import numpy as np
import pandas as pd
import streamlit as st

os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

from core.config.paths import (
    PATH_TELEGRAM_RAW,
    PATH_TELEGRAM_CLEAN,
    PATH_TELEGRAM_TRANSFORM,
    PATH_TELEGRAM_FILTER,
    PATH_TWITTER_RAW,
    PATH_TWITTER_CLEAN,
    PATH_TWITTER_FILTER,
    PATH_FILTER_SOCIAL_MEDIA,
    PATH_PRE_CLASSIFY_SOCIAL_MEDIA,
    PATH_CLASSIFY_SOCIAL_MEDIA,
)
from core.config.variables import (
    LIST_REGIONS,
    LIST_LAWS,
)
from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
    SCHEMA_EXCEL_SABOTAGE,
)


st.set_page_config(page_title="Gestion Data", page_icon=":gear:", layout="wide")


list_folders = [
    PATH_TELEGRAM_RAW,
    PATH_TELEGRAM_CLEAN,
    PATH_TELEGRAM_TRANSFORM,
    PATH_TELEGRAM_FILTER,
    PATH_TWITTER_RAW,
    PATH_TWITTER_CLEAN,
    PATH_TWITTER_FILTER,
    PATH_FILTER_SOCIAL_MEDIA,
    PATH_PRE_CLASSIFY_SOCIAL_MEDIA,
    PATH_CLASSIFY_SOCIAL_MEDIA,
]


with st.sidebar:

    # Select Folder
    st.write("Select Folder")
    select_folder = st.selectbox(
        "Select Folder",
        list_folders,
    )

    # Select File
    st.write("Select File")
    files = os.listdir(select_folder)
    files = [""] + files
    select_file = st.selectbox("Select File", files)

    st.divider()


if select_file == "":
    st.stop()
else:
    # Read Data
    if "df" not in st.session_state:
        st.session_state.df = pd.read_parquet(os.path.join(select_folder, select_file))

    if "df_filter" not in st.session_state:
        st.session_state.df_filter = None


with st.sidebar:
    st.write("Filters")

    # Select Columns
    st.write("Select Columns")
    select_columns = st.multiselect("Select Columns", st.session_state.df.columns)

    # Generate Filters
    for column in select_columns:
        st.write(column)
        st.write(st.session_state.df[column].dtype)

        if st.session_state.df[column].dtype == "object":
            if "region" in column:
                st.multiselect(
                    column, st.session_state.df[column].unique(), key=f"filter_{column}"
                )
            elif "law" in column:
                st.multiselect(
                    column, st.session_state.df[column].unique(), key=f"filter_{column}"
                )
            else:
                st.text_input(column, key=f"filter_{column}")

        elif st.session_state.df[column].dtype == "int64":
            st.number_input(column, step=1, key=f"filter_{column}")

        elif st.session_state.df[column].dtype == "float64":
            st.number_input(column, step=1, key=f"filter_{column}")

        elif st.session_state.df[column].dtype == "datetime64[ns]":
            st.date_input(column, key=f"filter_{column}")

        elif st.session_state.df[column].dtype == "bool":
            st.checkbox(column, key=f"filter_{column}")

        else:
            st.write("Not Found")

    st.divider()

    if st.button("Apply Filters"):
        st.session_state.df_filter = st.session_state.df.copy()

        for column in select_columns:
            if st.session_state.df[column].dtype == "object":
                if "region" in column:
                    st.session_state.df_filter = st.session_state.df_filter[
                        st.session_state.df_filter[column].isin(
                            st.session_state[f"filter_{column}"]
                        )
                    ]
                elif "law" in column:
                    st.session_state.df_filter = st.session_state.df_filter[
                        st.session_state.df_filter[column].isin(
                            st.session_state[f"filter_{column}"]
                        )
                    ]
                else:
                    st.session_state.df_filter = st.session_state.df_filter[
                        st.session_state.df_filter[column].str.contains(
                            st.session_state[f"filter_{column}"]
                        )
                    ]

            elif st.session_state.df[column].dtype == "int64":
                st.session_state.df_filter = st.session_state.df_filter[
                    st.session_state.df_filter[column]
                    == st.session_state[f"filter_{column}"]
                ]

            elif st.session_state.df[column].dtype == "float64":
                st.session_state.df_filter = st.session_state.df_filter[
                    st.session_state.df_filter[column]
                    == st.session_state[f"filter_{column}"]
                ]

            elif st.session_state.df[column].dtype == "datetime64[ns]":
                st.session_state.df_filter = st.session_state.df_filter[
                    st.session_state.df_filter[column]
                    == st.session_state[f"filter_{column}"]
                ]

            elif st.session_state.df[column].dtype == "bool":
                st.session_state.df_filter = st.session_state.df_filter[
                    st.session_state.df_filter[column]
                    == st.session_state[f"filter_{column}"]
                ]

            else:
                st.write("Not Found")

        st.session_state.df_filter = st.session_state.df_filter.reset_index(drop=True)

    if st.button("Reset Filters"):
        st.session_state.df_filter = None


with st.container():

    st.write("Data")
    if st.session_state.df_filter is not None:
        st.write(st.session_state.df_filter)
    else:
        st.write(st.session_state.df)

    col1, col2 = st.columns(2)
    with col1:
        # Show Data Describe
        st.write("Data Describe")
        st.write(st.session_state.df.describe())

    with col2:
        # Show Data Shape
        st.write("Data Types")
        st.write(st.session_state.df.dtypes)
