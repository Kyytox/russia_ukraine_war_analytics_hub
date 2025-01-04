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
from core.config.variables import (
    LIST_REGIONS,
    LIST_LAWS,
)
from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
    SCHEMA_EXCEL_SABOTAGE,
)

from utils.terms_arrest import list_exp_laws

st.set_page_config(page_title="Classify Data", page_icon=":bar_chart:", layout="wide")


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


def init_session_state():
    """
    Init session state
    """

    state = [
        "df_filter_source",
        "df_filter_pre_classify",
        "df_to_classify",
        "df_pre_classify",
        "df_classify_excel",
        #
        "path_filter_source",
        "path_pre_classify",
        "path_classify",
        #
        "schema_filter",
        "schema_pre_classify",
        "schema_excel",
        #
        "name_col_add_final",
        "name_col_filter",
        "found_terms",
    ]

    for s in state:
        if s not in st.session_state:
            st.session_state[s] = None

    st.session_state["list_express_laws"] = list_exp_laws


def init_state_type_data():
    """
    State type data
    """

    st.session_state["path_filter_source"] = (
        f"{PATH_FILTER_SOCIAL_MEDIA}/filter_social_media.parquet"
    )

    if st.session_state["type_data"] == "Incidents Railway":
        # Paths
        st.session_state["path_pre_classify"] = (
            f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_railway.parquet"
        )
        st.session_state["path_classify"] = (
            f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_railway.parquet"
        )

        # Schema
        st.session_state["schema_excel"] = SCHEMA_EXCEL_RAILWAY

        # Names cols
        st.session_state["name_col_add_final"] = "add_final_inc_railway"
        st.session_state["name_col_filter"] = "filter_inc_railway"
        st.session_state["found_terms"] = "found_terms_railway"

    elif st.session_state["type_data"] == "Incidents Arrest":
        # Paths
        st.session_state["path_pre_classify"] = (
            f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_arrest.parquet"
        )
        st.session_state["path_classify"] = (
            f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_arrest.parquet"
        )

        # Schema
        st.session_state["schema_excel"] = SCHEMA_EXCEL_ARREST

        # Names cols
        st.session_state["name_col_add_final"] = "add_final_inc_arrest"
        st.session_state["name_col_filter"] = "filter_inc_arrest"
        st.session_state["found_terms"] = "found_terms_arrest"

    elif st.session_state["type_data"] == "Incidents Sabotage":
        # Paths
        st.session_state["path_pre_classify"] = (
            f"{PATH_PRE_CLASSIFY_SOCIAL_MEDIA}/pre_classify_sabotage.parquet"
        )
        st.session_state["path_classify"] = (
            f"{PATH_CLASSIFY_SOCIAL_MEDIA}/classify_inc_sabotage.parquet"
        )

        # Schema
        st.session_state["schema_excel"] = SCHEMA_EXCEL_SABOTAGE

        # Names cols
        st.session_state["name_col_add_final"] = "add_final_inc_sabotage"
        st.session_state["name_col_filter"] = "filter_inc_sabotage"
        st.session_state["found_terms"] = "found_terms_sabotage"


def init_data():
    """
    Init data
    """

    # Init state type data
    init_state_type_data()

    # get data filter
    st.session_state["df_filter_source"] = pd.read_parquet(
        f"{st.session_state['path_filter_source']}"
    )
    st.session_state["schema_filter"] = st.session_state[
        "df_filter_source"
    ].dtypes.to_dict()

    # get data pre classify
    st.session_state["df_pre_classify"] = pd.read_parquet(
        f"{st.session_state['path_pre_classify']}"
    )
    st.session_state["schema_pre_classify"] = st.session_state[
        "df_pre_classify"
    ].dtypes.to_dict()

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
        .head(20)
        .copy()
    )

    # Get data classify (data final)
    if os.path.exists(f"{st.session_state['path_classify']}"):
        st.session_state["df_classify_excel"] = pd.read_parquet(
            f"{st.session_state['path_classify']}"
        )
        st.session_state["schema_excel"] = st.session_state[
            "df_classify_excel"
        ].dtypes.to_dict()
    else:
        st.session_state["df_classify_excel"] = pd.DataFrame()
        if st.session_state["type_data"] == "Incidents Railway":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_RAILWAY
        elif st.session_state["type_data"] == "Incidents Arrest":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_ARREST
        elif st.session_state["type_data"] == "Incidents Sabotage":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_SABOTAGE


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
        st.write(st.session_state[name_schema])
    else:
        schema = st.session_state[name_schema].dtypes.to_dict()
        st.write(st.session_state[name_schema].dtypes)

    # Select cols from schema
    df_new_data = df_new_data[schema.keys()]

    # Update types
    for col, dtype in schema.items():
        if col in df_new_data.columns:
            df_new_data[col] = df_new_data[col].astype(dtype)

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

    st.session_state[name_df].to_parquet(st.session_state[name_path_parquet])


def upd_filter_data():
    """
    Update filter data
    """
    # update df_filter with df_to_classify
    st.session_state["df_filter_source"] = (
        pd.concat(
            [
                st.session_state["df_filter_source"],
                st.session_state["df_to_classify"],
            ],
            ignore_index=True,
        )
        .drop_duplicates(subset=["ID"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # remove cols pre_
    cols_remove = [
        col for col in st.session_state["df_filter_source"].columns if "pre_" in col
    ] + ["IDX"]

    st.session_state["df_filter_source"] = remove_cols(
        st.session_state["df_filter_source"], cols_remove
    )

    # Update file parquet filter_social_media
    st.session_state["df_filter_source"].to_parquet(
        f"{PATH_FILTER_SOCIAL_MEDIA}/filter_social_media.parquet"
    )

    # reload elements and data
    init_data()
    st.rerun()


def upd_data_to_classify(id, col):
    """
    Update data to classify
    """

    print("--------------------------------")
    print(f"id: {id}")
    print(f"Col: {col}")
    print(f"Value: {st.session_state[f'{col}_{id}']}")

    if col == "IDX":
        if st.session_state[f"{col}_{id}"] == "arr_":
            return
        else:
            if not st.session_state["df_classify_excel"].empty:
                # get data IDX from classify
                df_IDX_exist = st.session_state["df_classify_excel"].loc[
                    st.session_state["df_classify_excel"]["IDX"]
                    == st.session_state[f"{col}_{id}"]
                ]

                if not df_IDX_exist.empty:
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

                    # Update the existing row with the new data
                    for sub_col in df_IDX_exist.columns:
                        if sub_col in st.session_state["df_to_classify"].columns:
                            st.session_state["df_to_classify"].loc[
                                st.session_state["df_to_classify"]["ID"] == id, sub_col
                            ] = df_IDX_exist[sub_col].values[0]

    # is list
    if isinstance(st.session_state[f"{col}_{id}"], list):
        if st.session_state[f"{col}_{id}"] == []:
            value = ""
        elif len(st.session_state[f"{col}_{id}"]) == 1:
            value = st.session_state[f"{col}_{id}"][0]
        else:
            value = ",".join(st.session_state[f"{col}_{id}"])
    else:
        value = st.session_state[f"{col}_{id}"]

    # update session state df
    st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == id, col
    ] = value

    # convert to int
    convert_cols(st.session_state["df_to_classify"])


def add_new_data_classify(ID):
    """
    Add new data to classify
    """

    # get data
    # df_new = pd.DataFrame(st.session_state["df_to_classify"].loc[index]).T
    df_new = st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID
    ]

    IDX = df_new["IDX"].values[0]

    # Check if IDX exists
    if IDX == "":
        st.error("IDX is required")
        return
    elif IDX == "arr_":
        st.error("IDX is required")
        return
    elif not st.session_state["df_classify_excel"].empty:
        if IDX in st.session_state["df_classify_excel"]["IDX"].values:
            st.error("IDX already exists")
            return

    # rename cols
    df_new_classify = df_new.rename(columns=lambda x: x.replace("pre_", ""))
    df_new_classify = df_new_classify.rename(columns={"url": "class_sources"})

    # update add_final_ col if False
    st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID,
        st.session_state["name_col_add_final"],
    ] = True

    # select cols and update types
    df_new_classify = select_cols_upd_types(df_new_classify, "schema_excel")

    # Concat new data and save data in parquet
    concat_new_data(df_new_classify, "df_classify_excel", "path_classify")

    #############
    # update data pre classify
    #############

    # Select cols and update types
    df_new_pre_classify = select_cols_upd_types(df_new, "schema_pre_classify")

    # Concat new data and save data in parquet
    concat_new_data(df_new_pre_classify, "df_pre_classify", "path_pre_classify")

    ################
    # update data filter
    ################

    # Select cols and update types
    df_filter = select_cols_upd_types(df_new, "schema_filter")

    # update add_final_ col if False
    df_filter[st.session_state["name_col_add_final"]] = True

    # Concat new data and save data in parquet
    concat_new_data(df_filter, "df_filter_source", "path_filter_source")

    init_data()
    st.rerun()


def upd_data_classify(ID_new_data):
    """
    Update data classify, add new url
    """

    # check IDX
    IDX = (
        st.session_state["df_to_classify"]
        .loc[st.session_state["df_to_classify"]["ID"] == ID_new_data, "IDX"]
        .values[0]
    )

    if IDX == "":
        st.error("IDX is required")
        return
    elif IDX == "arr_":
        st.error("IDX is required")
        return
    elif not st.session_state["df_classify_excel"].empty:
        if IDX not in st.session_state["df_classify_excel"]["IDX"].values:
            st.error("IDX not exists")
            return

    # get new data
    df_new = st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID_new_data
    ]
    st.write(df_new)

    ##############################
    ## UPDATE DATA PRE CLASSIFY ##
    ##############################

    # Select cols and update types
    df_new_pre_classify = select_cols_upd_types(df_new, "schema_pre_classify")

    # update all data for IDX
    for idx, row in st.session_state["df_pre_classify"].iterrows():
        if row["IDX"] == IDX:
            for col in df_new_pre_classify.columns:
                if col not in ["ID", "url"]:
                    if col in st.session_state["df_pre_classify"].columns:
                        st.session_state["df_pre_classify"].loc[
                            st.session_state["df_pre_classify"]["ID"] == row["ID"], col
                        ] = df_new_pre_classify[col].values[0]

    st.write(st.session_state["df_pre_classify"])

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
            df_new_classify[col] = f"{values_classify},{values_new}"

    # Concat new data and save data in parquet
    concat_new_data(df_new_classify, "df_classify_excel", "path_classify")

    # update add_final_ col if False
    st.session_state["df_to_classify"].loc[
        st.session_state["df_to_classify"]["ID"] == ID_new_data,
        st.session_state["name_col_add_final"],
    ] = True

    ########################
    ## UPDATE DATA FILTER ##
    ########################

    # updat add_final_ col if False
    st.session_state["df_filter_source"].loc[
        st.session_state["df_filter_source"]["ID"] == ID_new_data,
        st.session_state["name_col_add_final"],
    ] = True

    # update file parquet
    st.session_state["df_filter_source"].to_parquet(
        st.session_state["path_filter_source"]
    )

    # reload elements and data
    init_data()
    st.rerun()


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################

if "type_data" not in st.session_state:
    st.session_state["type_data"] = ""

if "saved_type_data" not in st.session_state:
    st.session_state["saved_type_data"] = ""

with st.sidebar:
    st.subheader("Filter data")

    st.session_state["type_data"] = st.selectbox(
        "Select Type Data",
        options=[
            "",
            "Incidents Railway",
            "Incidents Arrest",
            "Incidents Sabotage",
        ],
    )

    st.divider()


if st.session_state["type_data"] == "":
    st.stop()
elif st.session_state["saved_type_data"] != st.session_state["type_data"]:
    st.session_state["saved_type_data"] = st.session_state["type_data"]
    init_state_type_data()
    init_data()


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

if st.session_state["type_data"] == "Incidents Railway":
    next_idx = f"rail_{next_idx:02}"
elif st.session_state["type_data"] == "Incidents Arrest":
    next_idx = f"arr_{next_idx:02}"
elif st.session_state["type_data"] == "Incidents Sabotage":
    next_idx = f"sab_{next_idx:02}"


with st.sidebar:
    if st.session_state["type_data"] == "":
        st.stop()

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

    st.subheader("Data to classify")

    # update df_filter with df_to_classify
    if st.button("Update Filter Data", type="primary", use_container_width=True):
        upd_filter_data()

    st.dataframe(st.session_state["df_to_classify"], height=250)

    st.divider()

st.title(f"Classify Data - {st.session_state['type_data']}")

with st.expander("Filters", expanded=True):

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
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
        if st.session_state["type_data"] == "Incidents Railway":
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

        if st.session_state["type_data"] == "Incidents Arrest":
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

    with col3:

        # Apply Filter on df_filter_pre_classify
        if st.button("Apply Filter", type="primary", use_container_width=True):

            df_tmp = st.session_state["df_filter_pre_classify"]

            if filt_ia:
                df_tmp = df_tmp[df_tmp["pre_class_ia"] == True]

            if filt_reg:
                df_tmp = df_tmp[df_tmp["pre_class_region"] != ""]

            if st.session_state["type_data"] == "Incidents Railway":
                if filt_inc_type:
                    df_tmp = df_tmp[df_tmp["pre_class_incident_type"] != ""]
                if filt_dmg_eqp:
                    df_tmp = df_tmp[df_tmp["pre_class_dmg_equipment"] != ""]
                if filt_app_law:
                    df_tmp = df_tmp[df_tmp["pre_class_app_laws"] != ""]

            if st.session_state["type_data"] == "Incidents Arrest":
                if filt_reason:
                    df_tmp = df_tmp[df_tmp["pre_class_arrest_reason"] != ""]
                if filt_app_law:
                    df_tmp = df_tmp[df_tmp["pre_class_app_laws"] != ""]

            if filt_txt_cont:
                df_tmp = df_tmp[
                    df_tmp["text_translate"].str.contains(filt_txt_cont, na=False)
                    | df_tmp["text_original"].str.contains(filt_txt_cont, na=False)
                ]

            # Take the first 20 rows with add_final_ False and filter True
            st.session_state["df_to_classify"] = (
                df_tmp.loc[
                    (
                        (df_tmp[st.session_state["name_col_add_final"]] == False)
                        & (df_tmp[st.session_state["name_col_filter"]] == True)
                    )
                ]
                .head(20)
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
        col1, col2 = st.columns([0.4, 1.6])

        with col1:
            st.write(st.session_state[name_df].dtypes)
        with col2:
            st.data_editor(st.session_state[name_df])


#
with st.expander("Already Classified"):
    col1, col2 = st.columns([0.4, 1.6])

    with col1:
        st.write(st.session_state["df_classify_excel"].dtypes)

    with col2:
        st.data_editor(
            st.session_state["df_classify_excel"],
            # column_order=[
            #     "ID",
            #     "IDX",
            #     "class_region",
            #     "class_location",
            #     "class_arrest_date",
            #     "class_arrest_reason",
            #     "class_app_laws",
            #     "class_sentence_years",
            #     "class_sentence_date",
            #     "class_person_name",
            #     "class_person_age",
            #     "class_sources",
            #     "class_comments",
            # ],
            # column_config={
            #     "class_arrest_date": st.column_config.DatetimeColumn(
            #         format="MM.DD.YYYY", width="small"
            #     ),
            #     "class_sentence_date": st.column_config.DatetimeColumn(
            #         format="MM.DD.YYYY", width="small"
            #     ),
            #     "class_sentence_years": st.column_config.NumberColumn(width="small"),
            #     "class_person_age": st.column_config.NumberColumn(width="small"),
            # },
        )

st.divider()

for index, row in st.session_state["df_to_classify"].iterrows():

    text_message = (
        row["text_translate"]
        if row["text_translate"] != np.nan
        else row["text_original"]
    )

    ID = row["ID"]

    col1, col2, col3, col4 = st.columns([0.8, 1.5, 1, 1])

    with col1:
        # IDX
        st.write(f"IDX: {row['IDX']}")
        st.text_input(
            "IDX",
            # value=row["IDX"] if row["IDX"] != None else "arr_",
            value=row["IDX"] if pd.notnull(row["IDX"]) else "arr_",
            key=f"IDX_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "IDX"),
        )

        st.checkbox(
            "Filter Railway",
            value=row["filter_inc_railway"],
            key=f"filter_inc_railway_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "filter_inc_railway"),
        )

        st.checkbox(
            "Add Final Data Railway",
            value=row["add_final_inc_railway"],
            key=f"add_final_inc_railway_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "add_final_inc_railway"),
        )

        st.checkbox(
            "Filter Arrest",
            value=row["filter_inc_arrest"],
            key=f"filter_inc_arrest_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "filter_inc_arrest"),
        )

        st.checkbox(
            "Add Final Data Arrest",
            value=row[st.session_state["name_col_add_final"]],
            key=f"add_final_inc_arrest_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "add_final_inc_arrest"),
        )

        st.checkbox(
            "Filter Sabotage",
            value=row["filter_inc_sabotage"],
            key=f"filter_inc_sabotage_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "filter_inc_sabotage"),
        )

        st.checkbox(
            "Add Final Data Sabotage",
            value=row["add_final_inc_sabotage"],
            key=f"add_final_inc_sabotage_{ID}",
            on_change=upd_data_to_classify,
            args=(ID, "add_final_inc_sabotage"),
        )

        if st.button(
            "Update Pre Classify Data",
            type="primary",
            key=f"update_{ID}",
            use_container_width=True,
        ):
            upd_data_classify(ID)

        if st.button(
            "Add New Data", type="primary", key=f"add_{ID}", use_container_width=True
        ):
            add_new_data_classify(ID)

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
        for law in list_exp_laws:
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

    if st.session_state["type_data"] == "Incidents Railway":
        ################
        # RAILWAY
        ################
        with col3:
            # Date incident
            st.date_input(
                "Select Incident Date",
                value=(
                    row["pre_class_date_inc"]
                    if not pd.isnull(row["pre_class_date_inc"])
                    else None
                ),
                key=f"pre_class_date_inc_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_date_inc"),
                format="MM/DD/YYYY",
            )

            # Region
            st.selectbox(
                "Select Region",
                options=[
                    row["pre_class_region"],
                    *LIST_REGIONS,
                ],
                key=f"pre_class_region_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_region"),
            )

            # Incident Type
            st.selectbox(
                "Select Incident Type",
                options=[
                    (
                        row["pre_class_inc_type"]
                        if pd.notnull(row["pre_class_inc_type"])
                        else ""
                    )
                ],
                key=f"pre_class_inc_type_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_inc_type"),
            )

            # Coll With
            st.text_input(
                "Coll With",
                value=(
                    row["pre_class_coll_with"]
                    if pd.notnull(row["pre_class_coll_with"])
                    else ""
                ),
                key=f"pre_class_coll_with_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_coll_with"),
            )

            # Prtsn Arr
            st.toggle(
                "Prtsn Arr",
                value=row["pre_class_prtsn_arr"],
                key=f"pre_class_prtsn_arr_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_prtsn_arr"),
            )

            # Prtsn Age
            st.number_input(
                "Prtsn Age",
                value=(
                    row["pre_class_prtsn_age"]
                    if pd.notnull(row["pre_class_prtsn_age"])
                    else None
                ),
                key=f"pre_class_prtsn_age_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_prtsn_age"),
                step=1,
            )

        with col4:

            # GPS
            st.text_input(
                "GPS",
                value=(
                    row["pre_class_gps"] if pd.notnull(row["pre_class_gps"]) else ""
                ),
                key=f"pre_class_gps_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_gps"),
            )

            # Location
            st.text_input(
                "Location",
                value=(
                    row["pre_class_location"]
                    if pd.notnull(row["pre_class_location"])
                    else ""
                ),
                key=f"pre_class_location_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_location"),
            )

            # Damage Equipment
            st.text_input(
                "Damage Equipment",
                value=(
                    row["pre_class_dmg_eqp"]
                    if pd.notnull(row["pre_class_dmg_eqp"])
                    else ""
                ),
                key=f"pre_class_dmg_eqp_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_dmg_eqp"),
            )

            # Prtsn Group
            st.text_input(
                "Prtsn Group",
                value=(
                    row["pre_class_prtsn_grp"]
                    if pd.notnull(row["pre_class_prtsn_grp"])
                    else ""
                ),
                key=f"pre_class_prtsn_grp_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_prtsn_grp"),
            )

            # Prtsn Names
            st.text_input(
                "Prtsn Names",
                value=(
                    row["pre_class_prtsn_names"]
                    if pd.notnull(row["pre_class_prtsn_names"])
                    else ""
                ),
                key=f"pre_class_prtsn_names_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_prtsn_names"),
            )

            # Applied Laws
            st.multiselect(
                "Select Applied Laws",
                options=row["pre_class_app_laws"].split(",") + LIST_LAWS,
                default=(
                    row["pre_class_app_laws"].split(",")
                    if row["pre_class_app_laws"] != ""
                    else []
                ),
                key=f"pre_class_app_laws_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_app_laws"),
            )

            # Comments
            st.text_input(
                "Comments",
                value=(
                    row["pre_class_comments"]
                    if pd.notnull(row["pre_class_comments"])
                    else ""
                ),
                key=f"pre_class_comments_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_comments"),
            )

    elif st.session_state["type_data"] == "Incidents Arrest":
        ################
        # ARREST
        ################
        with col3:
            # Arrest Date
            st.date_input(
                "Select Arrest Date",
                value=(
                    row["pre_class_arrest_date"]
                    if not pd.isnull(row["pre_class_arrest_date"])
                    else None
                ),
                key=f"pre_class_arrest_date_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_arrest_date"),
                format="MM/DD/YYYY",
            )

            # Region
            st.selectbox(
                "Select Region",
                options=[
                    row["pre_class_region"],
                    *LIST_REGIONS,
                ],
                key=f"pre_class_region_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_region"),
            )

            # Reason Arrest
            st.selectbox(
                "Select Reason Arrest",
                options=[
                    (
                        row["pre_class_arrest_reason"]
                        if pd.notnull(row["pre_class_arrest_reason"])
                        else ""
                    ),
                    "Reason 2",
                    "Reason 3",
                    "Reason 4",
                    "Reason 5",
                ],
                key=f"pre_class_arrest_reason_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_arrest_reason"),
            )

            # Person Name
            st.text_input(
                "Person Name",
                value=(
                    row["pre_class_person_name"]
                    if pd.notnull(row["pre_class_person_name"])
                    else ""
                ),
                key=f"pre_class_person_name_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_person_name"),
            )

            # Person Age
            st.number_input(
                "Person Age",
                value=(
                    row["pre_class_person_age"]
                    if pd.notnull(row["pre_class_person_age"])
                    else None
                ),
                key=f"pre_class_person_age_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_person_age"),
                step=1,
            )

        with col4:

            # Date Sentence
            st.date_input(
                "Select Date Sentence",
                value=(
                    row["pre_class_sentence_date"]
                    if not pd.isnull(row["pre_class_sentence_date"])
                    else None
                ),
                key=f"pre_class_sentence_date_{ID}",
                format="MM/DD/YYYY",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_sentence_date"),
            )

            # Location
            st.text_input(
                "Location",
                value=(
                    row["pre_class_location"]
                    if pd.notnull(row["pre_class_location"])
                    else ""
                ),
                key=f"pre_class_location_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_location"),
            )

            # Applied Laws
            st.multiselect(
                "Select Applied Laws",
                options=row["pre_class_app_laws"].split(",") + LIST_LAWS,
                default=(
                    row["pre_class_app_laws"].split(",")
                    if row["pre_class_app_laws"] != ""
                    else []
                ),
                key=f"pre_class_app_laws_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_app_laws"),
            )

            # Sentence Years
            st.number_input(
                "Sentence Years",
                value=(
                    row["pre_class_sentence_years"]
                    if pd.notnull(row["pre_class_sentence_years"])
                    else None
                ),
                key=f"pre_class_sentence_years_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_sentence_years"),
                step=1,
            )

            # Comments
            st.text_input(
                "Comments",
                value=(
                    row["pre_class_comments"]
                    if pd.notnull(row["pre_class_comments"])
                    else ""
                ),
                key=f"pre_class_comments_{ID}",
                on_change=upd_data_to_classify,
                args=(ID, "pre_class_comments"),
            )

    elif st.session_state["type_data"] == "Incidents Sabotage":
        ################
        # SABOTAGE
        ################
        with col3:
            st.warning("TODO")

        with col4:
            st.warning("TODO")

    st.divider()
