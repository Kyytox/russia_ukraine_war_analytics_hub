import os
import numpy as np
import pandas as pd
import streamlit as st


from core.config.paths import (
    PATH_TELEGRAM_RAW,
    PATH_TELEGRAM_CLEAN,
    PATH_TELEGRAM_TRANSFORM,
    PATH_TELEGRAM_FILTER,
    #
    PATH_TWITTER_RAW,
    PATH_TWITTER_CLEAN,
    PATH_TWITTER_FILTER,
    #
    PATH_FILTER_DATALAKE,
    PATH_QUALIF_DATALAKE,
    PATH_CLASSIFY_DATALAKE,
)

from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
)


list_folders = [
    PATH_TELEGRAM_RAW,
    PATH_TELEGRAM_CLEAN,
    PATH_TELEGRAM_TRANSFORM,
    PATH_TELEGRAM_FILTER,
    #
    PATH_TWITTER_RAW,
    PATH_TWITTER_CLEAN,
    PATH_TWITTER_FILTER,
    #
    PATH_FILTER_DATALAKE,
    PATH_QUALIF_DATALAKE,
    PATH_CLASSIFY_DATALAKE,
]


if "df_tmp" not in st.session_state:
    st.session_state.df_tmp = pd.DataFrame()

if "df_final" not in st.session_state:
    st.session_state.df_final = pd.DataFrame()

if "df_raw" not in st.session_state:
    st.session_state.df_raw = pd.DataFrame()


def init_data(select_folder, select_file):
    """ """
    st.session_state.df_raw = pd.read_parquet(f"{select_folder}/{select_file}")


def save_data(df_edit, df_raw, path, type):
    """ """

    if type == "Remove":
        st.session_state.df_final = df_edit

    elif type == "Update":
        if "filter" in path:
            duplic_cols = ["ID"]
        elif "qualif" in path:
            duplic_cols = ["ID", "IDX"]
        elif "classify" in path:
            duplic_cols = ["IDX"]

        # find col with "date"
        sort_cols = [col for col in df_raw.columns if "date" in col.lower()]

        st.session_state.df_final = (
            pd.concat([df_raw, df_edit])
            .drop_duplicates(subset=duplic_cols, keep="last")
            .sort_values(by=sort_cols)
            .reset_index(drop=True)
        )

    st.write(st.session_state.df_final)
    st.rerun()


st.title("Gestion Data")

with st.sidebar:
    """"""

    # Type
    options = ["Update", "Remove"]
    select_type = st.segmented_control("Directions", options, selection_mode="single")

    if select_type:
        # Source
        select_folder = st.selectbox("Select a source", list_folders)
        select_file = st.selectbox("Select a file", [""] + os.listdir(select_folder))

        if select_file:
            init_data(select_folder, select_file)

            if select_type == "Update":
                st.divider()
                list_cols = ["ID", "IDX"]

                if "ID" in st.session_state.df_raw.columns:
                    select_id = st.text_input("ID")

                if "IDX" in st.session_state.df_raw.columns:
                    select_idx = st.selectbox(
                        "IDX", options=[""] + st.session_state.df_raw["IDX"].unique()
                    )

                if st.button("Filter", type="secondary", use_container_width=True):

                    if select_id:
                        st.session_state.df_tmp = st.session_state.df_raw[
                            st.session_state.df_raw["ID"] == select_id
                        ]

                    if select_idx:
                        st.session_state.df_tmp = st.session_state.df_raw[
                            st.session_state.df_raw["IDX"] == select_idx
                        ]

            if select_type == "Remove":
                st.session_state.df_tmp = st.session_state.df_raw.copy()


if st.session_state.df_final.shape[0] > 0:

    st.subheader(f"Data Final")
    st.write(st.session_state.df_final)

    if st.button("Comfirm", type="primary", use_container_width=True, key="confirm"):
        st.session_state.df_final.to_parquet(
            f"{select_folder}/{select_file}",
            index=False,
        )

        init_data(select_folder, select_file)

        st.session_state.df_tmp = pd.DataFrame()
        st.session_state.df_final = pd.DataFrame()

        st.rerun()

    st.stop()

if select_type and select_file:

    st.subheader(f"File: {select_file}")

    if select_type == "Update":
        if st.session_state.df_tmp.shape[0] > 0:
            df_edit = st.data_editor(
                st.session_state.df_tmp, num_rows="dynamic", use_container_width=True
            )
        else:
            df_edit = st.data_editor(
                st.session_state.df_raw, num_rows="dynamic", use_container_width=True
            )

    else:
        df_edit = st.data_editor(
            st.session_state.df_tmp, num_rows="dynamic", use_container_width=True
        )

    if st.button("Save", type="primary", use_container_width=True, key="save"):
        save_data(
            df_edit,
            st.session_state.df_raw,
            f"{select_folder}/{select_file}",
            select_type,
        )
