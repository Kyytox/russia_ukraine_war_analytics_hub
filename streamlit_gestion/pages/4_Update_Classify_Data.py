import os
import sys
import streamlit as st
import pandas as pd
import numpy as np

os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

from core.config.paths import (
    PATH_FILTER_DATALAKE,
    PATH_PRE_CLASSIFY_DATALAKE,
    PATH_CLASSIFY_DATALAKE,
)
from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
    SCHEMA_EXCEL_SABOTAGE,
)

from streamlit_gestion.utils.variables import LIST_EXP_LAWS, DICT_REF_INPUT_CLASS

st.set_page_config(page_title="Classify Data", page_icon=":bar_chart:", layout="wide")

HEAD_CLASS = 50


def init_state_theme_data():
    """
    State type data
    """

    st.session_state["path_filter_source"] = (
        f"{PATH_FILTER_DATALAKE}/filter_DATALAKE.parquet"
    )

    theme_data_config = {
        "Incidents Railway": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_DATALAKE}/pre_classify_railway.parquet",
            "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_railway.parquet",
            "schema_excel": SCHEMA_EXCEL_RAILWAY,
            "name_col_add_final": "add_final_inc_railway",
            "name_col_filter": "filter_inc_railway",
            "found_terms": "found_terms_railway",
            "name_col_date_class": "class_date_inc",
        },
        "Incidents Arrest": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_DATALAKE}/pre_classify_arrest.parquet",
            "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_arrest.parquet",
            "schema_excel": SCHEMA_EXCEL_ARREST,
            "name_col_add_final": "add_final_inc_arrest",
            "name_col_filter": "filter_inc_arrest",
            "found_terms": "found_terms_arrest",
            "name_col_date_class": "class_arrest_date",
        },
        "Incidents Sabotage": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_DATALAKE}/pre_classify_sabotage.parquet",
            "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_sabotage.parquet",
            "schema_excel": SCHEMA_EXCEL_SABOTAGE,
            "name_col_add_final": "add_final_inc_sabotage",
            "name_col_filter": "filter_inc_sabotage",
            "found_terms": "found_terms_sabotage",
            "name_col_date_class": "class_date_sab",
        },
    }

    config = theme_data_config.get(st.session_state["theme_data"], {})
    for key, value in config.items():
        st.session_state[key] = value


def init_data():
    """
    Init data
    """

    st.session_state["saved_select_IDX"] = ""

    st.session_state["df_temp_filter"] = pd.DataFrame()
    st.session_state["df_temp_pre_classify"] = pd.DataFrame()
    st.session_state["df_temp_classify"] = pd.DataFrame()

    # get data filter
    read_data("path_filter_source", "df_filter_source", "schema_filter")

    # get data pre classify
    read_data("path_pre_classify", "df_pre_classify", "schema_pre_classify")

    # Get data classify (data final)
    if os.path.exists(f"{st.session_state['path_classify']}"):
        read_data("path_classify", "df_classify_excel", "schema_excel")
    else:
        st.session_state["df_classify_excel"] = pd.DataFrame()
        if st.session_state["theme_data"] == "Incidents Railway":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_RAILWAY
        elif st.session_state["theme_data"] == "Incidents Arrest":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_ARREST
        elif st.session_state["theme_data"] == "Incidents Sabotage":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_SABOTAGE


def read_data(path_name, df_name, schema_name):
    """
    Read data and get schema

    Args:
        path_name: path to file in session state
        df_name: dataframe name in session state
        schema_name: schema name in session state
    """

    # read
    st.session_state[df_name] = pd.read_parquet(f"{st.session_state[path_name]}")

    # schema
    st.session_state[schema_name] = st.session_state[df_name].dtypes.to_dict()


def save_data(dict_save):
    """
    Save data in parquet

    Args:
        path: path to save parquet
        df_name: dataframe name in session state
    """

    # concat filter
    st.session_state["df_filter_source"] = (
        pd.concat(
            [st.session_state["df_filter_source"], st.session_state["df_temp_filter"]]
        )
        .drop_duplicates("ID", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # concat pre classify
    st.session_state["df_pre_classify"] = (
        pd.concat(
            [
                st.session_state["df_pre_classify"],
                st.session_state["df_temp_pre_classify"],
            ]
        )
        .drop_duplicates(["ID", "IDX"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # concat classify
    st.session_state["df_classify_excel"] = (
        pd.concat(
            [
                st.session_state["df_classify_excel"],
                st.session_state["df_temp_classify"],
            ]
        )
        .drop_duplicates("IDX", keep="last")
        .sort_values(st.session_state["name_col_date_class"])
        .reset_index(drop=True)
    )

    # st.dataframe(st.session_state["df_filter_source"])
    # st.dataframe(st.session_state["df_pre_classify"])
    # st.dataframe(st.session_state["df_classify_excel"])

    for path, df in dict_save.items():
        st.session_state[df].to_parquet(st.session_state[path])

    init_data()
    # st.rerun()


def convert_cols(df):
    """
    Convert cols

    Args:
        df: dataframe
        cols: list of cols
        dtype: type

    Returns:
        Dataframe with cols converted
    """

    # convert to int
    for col in df.select_dtypes(include=["float"]).columns:
        df[col] = df[col].fillna(0).astype(int)


def upd_pre_class_and_class(idx, col_name):
    """
    Update data to classify
    """

    print("--------------------------------")
    print(f"idx: {idx}")
    print(f"col Name: {col_name}")
    print(f"Value: {st.session_state[f'REF_{col_name}_{idx}']}")

    ###########################
    ## UPD TEMP PRE CLASSIFY ##
    ###########################
    if col_name != "IDX":
        pre_col_name = f"pre_{col_name}"

    # upd col_name, according to idx
    st.session_state["df_temp_pre_classify"].loc[
        st.session_state["df_temp_pre_classify"]["IDX"] == idx, pre_col_name
    ] = st.session_state[f"REF_{col_name}_{idx}"]

    #######################
    ## UPD TEMP CLASSIFY ##
    #######################

    # upd col_name, according to idx
    st.session_state["df_temp_classify"].loc[
        st.session_state["df_temp_classify"]["IDX"] == idx, col_name
    ] = st.session_state[f"REF_{col_name}_{idx}"]

    st.rerun()


def upd_filter(id, col_name):
    """
    Update data to classify
    """

    print("--------------------------------")
    print(f"id: {id}")
    print(f"col Name: {col_name}")
    print(f"Value: {st.session_state[f'REF_{col_name}_{id}']}")

    # upd col_name, according to idx
    st.session_state["df_temp_filter"].loc[
        st.session_state["df_temp_filter"]["ID"] == id, col_name
    ] = st.session_state[f"REF_{col_name}_{id}"]

    st.rerun()


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################

if "theme_data" not in st.session_state:
    st.session_state["theme_data"] = ""

if "saved_theme_data" not in st.session_state:
    st.session_state["saved_theme_data"] = ""

with st.sidebar:
    st.subheader("Filter data")

    st.session_state["theme_data"] = st.selectbox(
        "Select Type Data",
        options=[
            "",
            "Incidents Railway",
            "Incidents Arrest",
            "Incidents Sabotage",
        ],
    )

    st.divider()


if st.session_state["theme_data"] == "":
    st.stop()
elif st.session_state["saved_theme_data"] != st.session_state["theme_data"]:
    st.session_state["saved_theme_data"] = st.session_state["theme_data"]
    init_state_theme_data()
    init_data()
    st.rerun()

with st.expander("Already Classified"):
    col1, col2 = st.columns([0.4, 1.6])

    with col1:
        st.write(st.session_state["df_classify_excel"].dtypes)

    with col2:
        st.data_editor(
            st.session_state["df_classify_excel"],
        )

with st.sidebar:
    if st.session_state["theme_data"] == "":
        st.stop()

    # Select IDX
    select_IDX = st.selectbox(
        "Select IDX",
        options=[""] + st.session_state["df_classify_excel"]["IDX"].tolist(),
    )

    # get data IDX
    if select_IDX == "":
        st.warning("Select IDX to update data")
        st.stop()

    if st.session_state["saved_select_IDX"] != select_IDX:
        # get data with ID (multiple)
        list_ID = (
            st.session_state["df_classify_excel"]
            .loc[st.session_state["df_classify_excel"]["IDX"] == select_IDX, "ID"]
            .values[0]
            .split(",")
        )
        st.write(f"ID: {list_ID}")

        # For Filter get data with ID
        st.session_state["df_temp_filter"] = (
            st.session_state["df_filter_source"]
            .loc[st.session_state["df_filter_source"]["ID"].isin(list_ID)]
            .reset_index(drop=True)
        )

        # For Pre Classify get data with ID
        st.session_state["df_temp_pre_classify"] = (
            st.session_state["df_pre_classify"]
            .loc[
                (
                    st.session_state["df_pre_classify"]["ID"].isin(list_ID)
                    & (st.session_state["df_pre_classify"]["IDX"] == select_IDX)
                )
            ]
            .reset_index(drop=True)
        )

        # For Classify get data with IDX
        st.session_state["df_temp_classify"] = (
            st.session_state["df_classify_excel"]
            .loc[st.session_state["df_classify_excel"]["IDX"] == select_IDX]
            .reset_index(drop=True)
        )

        st.session_state["saved_select_IDX"] = select_IDX

    st.button(
        "Save Data",
        type="primary",
        use_container_width=True,
        on_click=save_data,
        args=[
            {
                "path_filter_source": "df_filter_source",
                "path_pre_classify": "df_pre_classify",
                "path_classify": "df_classify_excel",
            }
        ],
    )

    st.subheader("Variables")

    data = {
        "Remaining Data": st.session_state["df_filter_source"]
        .loc[
            st.session_state["df_filter_source"][st.session_state["name_col_filter"]]
            == True
        ]
        .shape[0],
        "Filter Source": st.session_state["path_filter_source"].split("/")[-1],
        "Pre Classify": st.session_state["path_pre_classify"].split("/")[-1],
        "Classify": st.session_state["path_classify"].split("/")[-1],
        "Name Col Add Final": st.session_state["name_col_add_final"],
    }

    st.table(data)
    st.divider()


st.title(f"Update Classify Data - {st.session_state['theme_data']}")


# with st.expander("All Data"):
#     names_df = [
#         "df_filter_source",
#         "df_pre_classify",
#         "df_filter_pre_classify",
#         "df_to_classify",
#     ]
#     for name_df in names_df:
#         st.subheader(name_df)
#         col1, col2 = st.columns([0.4, 1.6])

#         with col1:
#             st.write(st.session_state[name_df].dtypes)
#         with col2:
#             st.data_editor(st.session_state[name_df].head(50))


#

st.write("Classify")
st.dataframe(st.session_state["df_temp_classify"])
st.write("Filter")
st.dataframe(st.session_state["df_temp_filter"])
st.write("Pre Classify")
st.dataframe(st.session_state["df_temp_pre_classify"])

st.divider()


nb_cols = st.session_state["df_temp_filter"].shape[0]
cols = st.columns(nb_cols, border=True)

for index, row in st.session_state["df_temp_filter"].iterrows():

    ID = row["ID"]

    with cols[index]:

        col1, col2 = st.columns([1, 1])

        for dict_ref in DICT_REF_INPUT_CLASS:
            if dict_ref["name"] in row:
                col = eval(dict_ref["st_col"])

                with col:
                    if dict_ref["type"] == "toggle":
                        st.toggle(
                            dict_ref["label"],
                            value=row[dict_ref["name"]],
                            key=f"REF_{dict_ref['name']}_{ID}",
                            on_change=upd_filter,
                            args=(ID, dict_ref["name"]),
                        )

        st.write(row["text_translate"])
        st.write(row["url"])

st.divider()
for index, row in st.session_state["df_temp_classify"].iterrows():

    IDX = row["IDX"]

    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])

    for dict_ref in DICT_REF_INPUT_CLASS:
        if dict_ref["name"] in row:

            col = eval(dict_ref["st_col"])

            with col:
                if dict_ref["type"] == "text_input":
                    st.text_input(
                        dict_ref["label"],
                        value=(row[dict_ref["name"]]),
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "checkbox":
                    st.checkbox(
                        dict_ref["label"],
                        value=row[dict_ref["name"]],
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "date_input":
                    st.date_input(
                        dict_ref["label"],
                        value=(
                            row[dict_ref["name"]]
                            if not pd.isnull(row[dict_ref["name"]])
                            else None
                        ),
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                        format="MM/DD/YYYY",
                    )

                elif dict_ref["type"] == "selectbox":
                    st.selectbox(
                        dict_ref["label"],
                        options=(
                            [row[dict_ref["name"]], *dict_ref["options"]]
                            if row[dict_ref["name"]] != None
                            else ["", *dict_ref["options"]]
                        ),
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "toggle":
                    st.toggle(
                        dict_ref["label"],
                        value=row[dict_ref["name"]],
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "number_input":
                    st.number_input(
                        dict_ref["label"],
                        value=(
                            row[dict_ref["name"]]
                            if not pd.isnull(row[dict_ref["name"]])
                            else None
                        ),
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "multiselect":
                    st.multiselect(
                        dict_ref["label"],
                        options=[row[dict_ref["name"]], *dict_ref["options"]],
                        default=(
                            [row[dict_ref["name"]]] if row[dict_ref["name"]] else []
                        ),
                        key=f"REF_{dict_ref['name']}_{IDX}",
                        on_change=upd_pre_class_and_class,
                        args=(IDX, dict_ref["name"]),
                    )

    st.divider()

st.stop()
