import os
import sys
import streamlit as st
import pandas as pd
import numpy as np

os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

from core.config.paths import (
    PATH_FILTER_SOCIAL_MEDIA,
    PATH_PRE_CLASSIFY_SOCIAL_MEDIA,
    PATH_CLASSIFY_SOCIAL_MEDIA,
)
from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
    SCHEMA_EXCEL_SABOTAGE,
)

from streamlit_gestion.utils.variables import LIST_EXP_LAWS, DICT_REF_INPUT

st.set_page_config(page_title="Classify Data", page_icon=":bar_chart:", layout="wide")

HEAD_CLASS = 50


def init_state_theme_data():
    """
    State type data
    """

    st.session_state["path_filter_source"] = (
        f"{PATH_FILTER_SOCIAL_MEDIA}/filter_social_media.parquet"
    )

    theme_data_config = {
        "Incidents Railway": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_railway.parquet",
            "path_classify": f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_railway.parquet",
            "schema_excel": SCHEMA_EXCEL_RAILWAY,
            "name_col_add_final": "add_final_inc_railway",
            "name_col_filter": "filter_inc_railway",
            "found_terms": "found_terms_railway",
        },
        "Incidents Arrest": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_arrest.parquet",
            "path_classify": f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_arrest.parquet",
            "schema_excel": SCHEMA_EXCEL_ARREST,
            "name_col_add_final": "add_final_inc_arrest",
            "name_col_filter": "filter_inc_arrest",
            "found_terms": "found_terms_arrest",
        },
        "Incidents Sabotage": {
            "path_pre_classify": f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_sabotage.parquet",
            "path_classify": f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_sabotage.parquet",
            "schema_excel": SCHEMA_EXCEL_SABOTAGE,
            "name_col_add_final": "add_final_inc_sabotage",
            "name_col_filter": "filter_inc_sabotage",
            "found_terms": "found_terms_sabotage",
        },
    }

    config = theme_data_config.get(st.session_state["theme_data"], {})
    for key, value in config.items():
        st.session_state[key] = value


def init_data():
    """
    Init data
    """

    # get data filter
    read_data("path_filter_source", "df_filter_source", "schema_filter")

    # get data pre classify
    read_data("path_pre_classify", "df_pre_classify", "schema_pre_classify")

    # merge data filter and pre classify
    st.session_state["df_filter_pre_classify"] = pd.merge(
        st.session_state["df_filter_source"].drop(columns=["date", "url"]),
        st.session_state["df_pre_classify"],
        on=["ID"],
        how="left",
    )

    convert_cols(st.session_state["df_filter_pre_classify"])

    # Take the first 20 rows with add_final_ False and filter True
    st.session_state["df_to_classify"] = (
        st.session_state["df_filter_pre_classify"]
        .loc[
            (
                (
                    st.session_state["df_filter_pre_classify"][
                        st.session_state["name_col_add_final"]
                    ]
                    == False
                )
                & (
                    st.session_state["df_filter_pre_classify"][
                        st.session_state["name_col_filter"]
                    ]
                    == True
                )
            )
        ]
        .head(HEAD_CLASS)
        .copy()
    )

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

    for path, df in dict_save.items():
        st.session_state[df].to_parquet(st.session_state[path])

    init_data()
    st.rerun()


def get_cols_lists(df):
    """
    Get cols lists

    Args:
        df: dataframe

    Returns:
        Tuple with lists of cols
    """

    cols_origin = ["date", "text_original", "text_translate", "class_ia", "url"]
    cols_filter = [col for col in df.columns if "filter_" in col]
    cols_add_final = [col for col in df.columns if "add_final_" in col]
    cols_found_terms = [col for col in df.columns if "found_terms_" in col]
    cols_pre_class = [col for col in df.columns if "pre_class_" in col]

    return cols_origin, cols_filter, cols_add_final, cols_found_terms, cols_pre_class


def remove_cols(df, cols_to_remove):
    """
    Remove cols

    Args:
        df: dataframe
        cols_to_remove: list of cols to remove

    Returns:
        Dataframe without cols
    """

    for col in cols_to_remove:
        if col in df.columns:
            df = df.drop(col, axis=1)

    return df


def select_cols_upd_types(df_new_data, name_schema):
    """
    Select cols and update types

    Args:
        df_new_data: dataframe with new data
        name_schema: name of schema or name of dataframe

    Returns:
        df_new_data: dataframe with new data
    """

    if name_schema.startswith("schema"):
        schema = st.session_state[name_schema]
    else:
        schema = st.session_state[name_schema].dtypes.to_dict()

    # Select cols from schema
    df_new_data = df_new_data[schema.keys()]

    # Update types
    for col, dtype in schema.items():
        if col in df_new_data.columns:
            df_new_data.loc[:, col] = df_new_data[col].astype(dtype)

    return df_new_data


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


def concat_new_data(df_new_data, name_df, name_path_parquet):
    """
    Concat new data
    Save data in parquet

    Args:
        df_new_data: dataframe with new data
        name_df: name of dataframe
        path_parquet: path to save parquet

    """

    # Add new data
    if name_path_parquet == "path_classify":
        # Add new data to df_classify_excel
        st.session_state[name_df] = pd.concat(
            [st.session_state[name_df], df_new_data], ignore_index=True
        ).drop_duplicates(subset=["IDX"], keep="last")
    elif name_path_parquet == "path_pre_classify":
        st.session_state[name_df] = (
            pd.concat(
                [
                    st.session_state[name_df],
                    df_new_data,
                ],
                ignore_index=True,
            )
            .drop_duplicates(subset=["ID", "IDX"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )
    else:
        st.session_state[name_df] = (
            pd.concat(
                [
                    st.session_state[name_df],
                    df_new_data,
                ],
                ignore_index=True,
            )
            .drop_duplicates(subset=["ID"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )


def upd_ref_input(id, col_name):
    """
    Update data to classify
    """

    print("--------------------------------")
    print(f"id: {id}")
    print(f"col Name: {col_name}")
    print(f"Value: {st.session_state[f'REF_{col_name}_{id}']}")

    if col_name == "IDX":
        IDX = st.session_state[f"REF_{col_name}_{id}"]

        if not st.session_state["df_classify_excel"].empty:
            if IDX in st.session_state["df_classify_excel"]["IDX"].values:

                # get data IDX from classify
                df_IDX_exist = st.session_state["df_classify_excel"].loc[
                    st.session_state["df_classify_excel"]["IDX"]
                    == st.session_state[f"REF_{col_name}_{id}"]
                ]
                print(f"df_IDX_exist: {df_IDX_exist}")

                # Rename columns with prefix 'class_' to 'pre_class_'
                df_IDX_exist = df_IDX_exist.rename(
                    columns=lambda col: (
                        f"pre_{col}" if col.startswith("class_") else col
                    )
                )

                # remove sources
                df_IDX_exist = df_IDX_exist.drop(columns=["pre_class_sources"])

                # add ID
                df_IDX_exist["ID"] = id

                print(f"df_IDX_exist: {df_IDX_exist}")
                # Update the existing row with the new data
                for sub_col in df_IDX_exist.columns:
                    if sub_col in st.session_state["df_to_classify"].columns:
                        print(f"Sub col: {sub_col}")
                        st.session_state["df_to_classify"].loc[
                            (st.session_state["df_to_classify"]["ID"] == id),
                            sub_col,
                        ] = df_IDX_exist[sub_col].values[0]


def define_new_data(ID, date, url):
    """
    Define new data

    Args:
        ID: ID
        date: date
        url: url
    """

    print("--------------------------------")
    print("Define new data")
    print(f"ID: {ID}")
    print(f"Date: {date}")
    print(f"URL: {url}")

    # get REF session state from ID
    refs = [
        key
        for key in st.session_state.keys()
        if key.startswith(f"REF_") and key.endswith(f"_{ID}")
    ]

    # transform ref to df
    df_new = pd.DataFrame()
    for ref in refs:
        col = ref.replace(f"_{ID}", "").replace("REF_", "")
        value = st.session_state[ref]
        print(f"Ref: {ref} - Col: {col} - Value: {value}")
        df_new[col] = "" if isinstance(value, list) else [value]

    # add dismissed cols
    df_new["ID"] = ID
    df_new["date"] = date
    df_new["pre_class_ia"] = "yes"
    df_new["url"] = url

    # update add_final_ and filter_inc_ col
    df_new[st.session_state["name_col_add_final"]] = True
    df_new[st.session_state["name_col_filter"]] = True

    return df_new


def add_new_data_classify(ID, date, url):
    """
    Add new data classify

    Args:
        ID: ID
        date: date
        url: url
    """
    print("--------------------------------")
    print("Add new data to classify")
    print(f"ID: {ID}")
    print(f"Date: {date}")
    print(f"URL: {url}")

    # Check if IDX exists
    IDX = st.session_state[f"REF_IDX_{ID}"]

    if IDX == "":
        st.error("IDX is required")
        return
    elif not IDX.startswith(prefix_idx):
        st.error("IDX not valid")
        return
    elif IDX == prefix_idx:
        st.error("IDX not valid")
        return
    elif not st.session_state["df_classify_excel"].empty:
        if IDX in st.session_state["df_classify_excel"]["IDX"].values:
            st.error("IDX already exists")
            return

    # Define new data
    df_new = define_new_data(ID, date, url)

    ##################
    ## ADD CLASSIFY ##
    ##################

    # rename cols
    df_new_classify = df_new.rename(columns=lambda x: x.replace("pre_", ""))
    df_new_classify = df_new_classify.rename(columns={"url": "class_sources"})

    # select cols and update types
    df_new_classify = select_cols_upd_types(df_new_classify, "schema_excel")

    # Concat new data and save data in parquet
    concat_new_data(df_new_classify, "df_classify_excel", "path_classify")

    #########################
    ## UPDATE PRE CLASSIFY ##
    #########################

    # Select cols and update types
    df_new_pre_classify = select_cols_upd_types(df_new, "schema_pre_classify")

    # remove from pre_classify row with ID == ID and IDX == ""
    st.session_state["df_pre_classify"] = st.session_state["df_pre_classify"].loc[
        ~(
            (st.session_state["df_pre_classify"]["ID"] == ID)
            # & (st.session_state["df_pre_classify"]["IDX"] == "")
            & (pd.isnull(st.session_state["df_pre_classify"]["IDX"]))
        )
    ]

    # Concat new data and save data in parquet
    concat_new_data(df_new_pre_classify, "df_pre_classify", "path_pre_classify")

    ########################
    ## UPDATE FILTER DATA ##
    ########################

    # update cols filter
    for index, row in df_new.iterrows():
        for col in df_new.columns:
            if col in st.session_state["df_filter_source"].columns:
                st.session_state["df_filter_source"].loc[
                    st.session_state["df_filter_source"]["ID"] == row["ID"], col
                ] = df_new[col].values[0]

    ########################
    ## UPDATE TO CLASSIFY ##
    ########################

    # update add_final_ col if False
    st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID,
        st.session_state["name_col_add_final"],
    ] = True


def upd_data_classify(ID, date, url):
    """
    Update data classify, add new url

    Args:
        ID: ID
        date: date
        url: url
    """
    print("--------------------------------")
    print("Update data to classify")
    print(f"ID: {ID}")
    print(f"Date: {date}")
    print(f"URL: {url}")

    # check IDX
    IDX = st.session_state[f"REF_IDX_{ID}"]
    if IDX == "":
        st.error("IDX is required")
        return
    elif not IDX.startswith(prefix_idx):
        st.error("IDX not valid")
        return
    elif IDX == prefix_idx:
        st.error("IDX not valid")
        return
    elif not st.session_state["df_classify_excel"].empty:
        if IDX not in st.session_state["df_classify_excel"]["IDX"].values:
            st.error("IDX already exists")
            return

    # Define new data
    df_new = define_new_data(ID, date, url)

    ##############################
    ## UPDATE DATA PRE CLASSIFY ##
    ##############################

    # Select cols and update types
    df_new_pre_classify = select_cols_upd_types(df_new, "schema_pre_classify")

    # update all data for IDX
    for index, row in st.session_state["df_pre_classify"].iterrows():
        if row["IDX"] == IDX:
            for col in df_new_pre_classify.columns:
                if col not in ["ID", "url"]:
                    if col in st.session_state["df_pre_classify"].columns:
                        st.session_state["df_pre_classify"].loc[
                            st.session_state["df_pre_classify"]["IDX"] == IDX, col
                        ] = df_new_pre_classify[col].values[0]

    # remove from pre_classify row with ID == ID and IDX == ""
    st.session_state["df_pre_classify"] = st.session_state["df_pre_classify"].loc[
        ~(
            (st.session_state["df_pre_classify"]["ID"] == ID)
            & (pd.isnull(st.session_state["df_pre_classify"]["IDX"]))
        )
    ]

    # Concat new data and save data in parquet
    concat_new_data(df_new_pre_classify, "df_pre_classify", "path_pre_classify")

    ##########################
    ## UPDATE DATA CLASSIFY ##
    ##########################

    # rename cols
    df_new_classify = df_new.rename(columns=lambda x: x.replace("pre_", ""))
    df_new_classify = df_new_classify.rename(columns={"url": "class_sources"})

    # select cols and update types
    df_new_classify = select_cols_upd_types(df_new_classify, "schema_excel")

    # concat data
    cols = ["ID", "class_sources"]
    for col in cols:

        # get values from classify
        values_classify = (
            st.session_state["df_classify_excel"]
            .loc[st.session_state["df_classify_excel"]["IDX"] == IDX, col]
            .values[0]
        )

        # get values from new data
        values_new = df_new_classify[col].values[0]

        # update values
        if values_classify == "":
            df_new_classify[col] = values_new
        else:
            if values_new not in values_classify:
                df_new_classify[col] = f"{values_classify},{values_new}"

    # Concat new data and save data in parquet
    concat_new_data(df_new_classify, "df_classify_excel", "path_classify")

    # update add_final_ col if False
    st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID,
        st.session_state["name_col_add_final"],
    ] = True

    ########################
    ## UPDATE DATA FILTER ##
    ########################

    # update cols filter
    for index, row in df_new.iterrows():
        for col in df_new.columns:
            if col in st.session_state["df_filter_source"].columns:
                st.session_state["df_filter_source"].loc[
                    st.session_state["df_filter_source"]["ID"] == row["ID"], col
                ] = df_new[col].values[0]


def apply_basic_filters(df, filters):
    """Applique les filtres de base"""
    mask = pd.Series(True, index=df.index)

    filter_conditions = {
        "filt_ia": ("pre_class_ia", "yes"),
        "filt_reg": ("pre_class_region", ""),
        "filt_inc_rail": ("filter_inc_railway", True),
        "filt_inc_arr": ("filter_inc_arrest", True),
        "filt_inc_sab": ("filter_inc_sabotage", True),
    }

    for filt_name, (col, value) in filter_conditions.items():
        if filters.get(filt_name):
            mask &= (
                (df[col] == value) if isinstance(value, bool) else (df[col] != value)
            )

    return mask


def apply_incident_filters(df, filters, incident_type):
    """Applique les filtres spécifiques aux incidents"""
    mask = pd.Series(True, index=df.index)

    incident_conditions = {
        "Incidents Railway": {
            "filt_inc_type": "pre_class_incident_type",
            "filt_dmg_eqp": "pre_class_dmg_equipment",
            "filt_app_law": "pre_class_app_laws",
        },
        "Incidents Arrest": {
            "filt_reason": "pre_class_arrest_reason",
            "filt_app_law": "pre_class_app_laws",
        },
    }

    if incident_type in incident_conditions:
        for filt_name, col in incident_conditions[incident_type].items():
            if filters.get(filt_name):
                mask &= df[col] != ""

    return mask


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


# max IDX
next_idx = (
    st.session_state["df_classify_excel"]["IDX"].str.split("_").str[1].astype(int).max()
    if not st.session_state["df_classify_excel"].empty
    else 0
)
if next_idx == 0:
    next_idx = 1
else:
    next_idx = next_idx + 1

if st.session_state["theme_data"] == "Incidents Railway":
    prefix_idx = "rail_"
    next_idx = f"rail_{next_idx:02}"
elif st.session_state["theme_data"] == "Incidents Arrest":
    prefix_idx = "arr_"
    next_idx = f"arr_{next_idx:02}"
elif st.session_state["theme_data"] == "Incidents Sabotage":
    prefix_idx = "sab_"
    next_idx = f"sab_{next_idx:02}"


with st.sidebar:
    if st.session_state["theme_data"] == "":
        st.stop()

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
        "Next IDX": next_idx,
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


st.title(f"Classify Data - {st.session_state['theme_data']}")


with st.expander("Filters", expanded=True):

    col1, col2, col3, col4 = st.columns([0.4, 0.4, 0.4, 0.4], border=True)

    with col1:
        st.subheader("Globals")

        # toggle claas ia
        filt_ia = st.toggle(
            "Class IA",
            value=False,
        )

        # toggle class region
        filt_reg = st.toggle(
            "Class Region",
            value=False,
        )

        # text contains
        filt_txt_cont = st.text_input(
            "Text Contains",
            value="",
        )

    with col2:
        st.subheader("Terms Found")
        filt_inc_rail = st.toggle(
            "Terms Railway",
            value=False,
        )
        filt_inc_arr = st.toggle(
            "Terms Arrest",
            value=False,
        )
        filt_inc_sab = st.toggle(
            "Terms Sabotage",
            value=False,
        )

    with col3:
        st.subheader("Specifics")

        if st.session_state["theme_data"] == "Incidents Railway":
            # toggle class incident type
            filt_inc_type = st.toggle(
                "Class Incident Type",
                value=False,
            )

            # toggle class dmg equipment
            filt_dmg_eqp = st.toggle(
                "Class Damage Equipment",
                value=False,
            )

            # toggle class app laws
            filt_app_law = st.toggle(
                "Class Applied Laws",
                value=False,
            )

        if st.session_state["theme_data"] == "Incidents Arrest":
            # toggle class reason
            filt_reason = st.toggle(
                "Class Reason",
                value=False,
            )

            # toggle class app laws
            filt_app_law = st.toggle(
                "Class Applied Laws",
                value=False,
            )

    with col4:

        if st.button("Reset Filters", type="secondary", use_container_width=True):
            # reload all
            init_data()
            st.rerun()

        # Apply Filter on df_filter_pre_classify
        if st.button("Apply Filter", type="primary", use_container_width=True):

            df_tmp = st.session_state["df_filter_pre_classify"]

            filters = {
                "filt_ia": filt_ia,
                "filt_reg": filt_reg,
                "filt_inc_rail": filt_inc_rail,
                "filt_inc_arr": filt_inc_arr,
                "filt_inc_sab": filt_inc_sab,
            }

            if st.session_state["theme_data"] == "Incidents Railway":
                # add filters
                filters = {
                    **filters,
                    "filt_inc_type": filt_inc_type,
                    "filt_dmg_eqp": filt_dmg_eqp,
                    "filt_app_law": filt_app_law,
                }

            elif st.session_state["theme_data"] == "Incidents Arrest":
                # add filters
                filters = {
                    **filters,
                    "filt_reason": filt_reason,
                    "filt_app_law": filt_app_law,
                }

            # Apply filters
            mask = apply_basic_filters(df_tmp, filters)
            mask &= apply_incident_filters(
                df_tmp, filters, st.session_state["theme_data"]
            )

            # Apply text filter
            if filt_txt_cont:
                mask &= df_tmp["text_translate"].str.contains(
                    filt_txt_cont, na=False
                ) | df_tmp["text_original"].str.contains(filt_txt_cont, na=False)

            # Apply filters and take 20 rows
            st.session_state["df_to_classify"] = (
                df_tmp[
                    mask
                    & (df_tmp[st.session_state["name_col_add_final"]] == False)
                    & (df_tmp[st.session_state["name_col_filter"]] == True)
                ]
                .head(HEAD_CLASS)
                .copy()
            )


with st.expander("All Data"):
    names_df = [
        "df_filter_source",
        "df_pre_classify",
        "df_filter_pre_classify",
        "df_to_classify",
    ]
    for name_df in names_df:
        st.subheader(name_df)
        col1, col2 = st.columns([0.4, 1.6], border=True)

        with col1:
            st.write(st.session_state[name_df].dtypes)
        with col2:
            st.data_editor(st.session_state[name_df].head(50))


#
with st.expander("Already Classified"):
    col1, col2 = st.columns([0.4, 1.6], border=True)

    with col1:
        st.write(st.session_state["df_classify_excel"].dtypes)

    with col2:
        st.data_editor(
            st.session_state["df_classify_excel"],
        )

st.divider()


for index, row in st.session_state["df_to_classify"].iterrows():

    text_message = (
        row["text_translate"]
        # if row["text_translate"]
        if row["text_translate"] != None
        else row["text_original"]
    )

    ID = row["ID"]

    col1, col2, col3, col4 = st.columns([0.8, 1.5, 1, 1], border=True)

    with col2:
        st.markdown(f"{ID} - :blue[{row['date']}]")
        st.markdown(f"{row['url']}")

        if row["pre_class_region"] != "":
            highlighted_text = text_message.replace(
                row["pre_class_region"],
                f":red[{row['pre_class_region']}]",
            )
        else:
            highlighted_text = text_message

        # Highlight text for Laws
        for law in LIST_EXP_LAWS:
            if law in text_message:
                highlighted_text = highlighted_text.replace(
                    law,
                    f":orange[{law}]",
                )

        found_terms = row[st.session_state["found_terms"]].split(",")
        for found_term in found_terms:
            if found_term in text_message and found_term != "":
                highlighted_text = highlighted_text.replace(
                    found_term,
                    f":green[{found_term}]",
                )

        st.markdown(highlighted_text, unsafe_allow_html=True)

    for dict_ref in DICT_REF_INPUT:
        if dict_ref["name"] in row:
            col = eval(dict_ref["st_col"])

            with col:
                if dict_ref["type"] == "text_input":

                    if dict_ref["name"] == "IDX":
                        st.text_input(
                            dict_ref["label"],
                            value=(
                                row[dict_ref["name"]]
                                if row[dict_ref["name"]] != None
                                else prefix_idx
                            ),
                            key=f"REF_{dict_ref['name']}_{ID}",
                            on_change=upd_ref_input,
                            args=(ID, dict_ref["name"]),
                        )
                    else:
                        st.text_input(
                            dict_ref["label"],
                            value=(row[dict_ref["name"]]),
                            key=f"REF_{dict_ref['name']}_{ID}",
                            on_change=upd_ref_input,
                            args=(ID, dict_ref["name"]),
                        )

                elif dict_ref["type"] == "checkbox":
                    st.checkbox(
                        dict_ref["label"],
                        value=row[dict_ref["name"]],
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "date_input":
                    st.date_input(
                        dict_ref["label"],
                        value=(
                            row[dict_ref["name"]]
                            if not pd.isnull(row[dict_ref["name"]])
                            else None
                        ),
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
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
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "toggle":
                    st.toggle(
                        dict_ref["label"],
                        value=row[dict_ref["name"]],
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "number_input":
                    st.number_input(
                        dict_ref["label"],
                        value=(
                            row[dict_ref["name"]]
                            if not pd.isnull(row[dict_ref["name"]])
                            else None
                        ),
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "multiselect":
                    st.multiselect(
                        dict_ref["label"],
                        options=[*dict_ref["options"]],
                        default=[row[dict_ref["name"]]],
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, dict_ref["name"]),
                    )

    col1.button(
        "Update Pre Classify Data",
        type="primary",
        key=f"BTN_update_{ID}",
        use_container_width=True,
        on_click=upd_data_classify,
        args=(ID, row["date"], row["url"]),
    )

    col1.button(
        "Add New Data",
        type="primary",
        key=f"BTN_add_{ID}",
        use_container_width=True,
        on_click=add_new_data_classify,
        args=(ID, row["date"], row["url"]),
    )

    st.divider()
