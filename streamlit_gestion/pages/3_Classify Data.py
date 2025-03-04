import os
import sys
import streamlit as st
import pandas as pd
import numpy as np

os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

from core.config.paths import (
    PATH_FILTER_DATALAKE,
    PATH_QUALIF_DATALAKE,
    PATH_CLASSIFY_DATALAKE,
)
from core.config.schemas import (
    SCHEMA_EXCEL_RAILWAY,
    SCHEMA_EXCEL_ARREST,
    # SCHEMA_EXCEL_SABOTAGE,
)

from streamlit_gestion.utils.variables import (
    LIST_EXP_LAWS,
    DICT_REF_INPUT_RAILWAY,
    DICT_REF_INPUT_ARREST,
)

st.set_page_config(page_title="Classify Data", page_icon=":bar_chart:", layout="wide")

HEAD_CLASS = 10


def init_state_theme_data():
    """
    State type data
    """

    st.session_state["path_filter_source"] = (
        f"{PATH_FILTER_DATALAKE}/filter_datalake.parquet"
    )

    theme_data_config = {
        "Incidents Railway": {
            "path_qualif": f"{PATH_QUALIF_DATALAKE}/qualification_railway.parquet",
            "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_railway.parquet",
            "schema_excel": SCHEMA_EXCEL_RAILWAY,
            "name_col_add_final": "add_final_inc_railway",
            "name_col_filter": "filter_inc_railway",
            "found_terms": "found_terms_railway",
            "dict_input": DICT_REF_INPUT_RAILWAY,
        },
        "Incidents Arrest": {
            "path_qualif": f"{PATH_QUALIF_DATALAKE}/qualification_arrest.parquet",
            "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_arrest.parquet",
            "schema_excel": SCHEMA_EXCEL_ARREST,
            "name_col_add_final": "add_final_inc_arrest",
            "name_col_filter": "filter_inc_arrest",
            "found_terms": "found_terms_arrest",
            "dict_input": DICT_REF_INPUT_ARREST,
        },
        # "Incidents Sabotage": {
        #     "path_qualif": f"{PATH_QUALIF_DATALAKE}/qualification_sabotage.parquet",
        #     "path_classify": f"{PATH_CLASSIFY_DATALAKE}/classify_inc_sabotage.parquet",
        #     "schema_excel": SCHEMA_EXCEL_SABOTAGE,
        #     "name_col_add_final": "add_final_inc_sabotage",
        #     "name_col_filter": "filter_inc_sabotage",
        #     "found_terms": "found_terms_sabotage",
        # },
    }

    config = theme_data_config.get(st.session_state["theme_data"], {})
    for key, value in config.items():
        st.session_state[key] = value


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


def init_raw_data():
    """
    Init raw data
    """

    # get data filter
    read_data("path_filter_source", "df_filt_src", "schema_filter")

    # get data qualif
    read_data("path_qualif", "df_qualif", "schema_qualif")

    # Get data classify (data final)
    if os.path.exists(f"{st.session_state['path_classify']}"):
        read_data("path_classify", "df_class_excel", "schema_excel")
    else:
        st.session_state["df_class_excel"] = pd.DataFrame()
        if st.session_state["theme_data"] == "Incidents Railway":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_RAILWAY
        elif st.session_state["theme_data"] == "Incidents Arrest":
            st.session_state["schema_excel"] = SCHEMA_EXCEL_ARREST
        # elif st.session_state["theme_data"] == "Incidents Sabotage":
        #     st.session_state["schema_excel"] = SCHEMA_EXCEL_SABOTAGE


def init_tmp_data():
    """
    Init tmp data
    """

    # merge data filter and qualif
    st.session_state["df_tmp_filt_qualif"] = (
        pd.merge(
            st.session_state["df_filt_src"]
            .loc[
                (
                    st.session_state["df_filt_src"][st.session_state["name_col_filter"]]
                    == True
                )
                & (
                    st.session_state["df_filt_src"][
                        st.session_state["name_col_add_final"]
                    ]
                    == False
                )
            ]
            .drop(columns=["date", "url"]),
            st.session_state["df_qualif"],
            on=["ID"],
            how="left",
        )
        .head(HEAD_CLASS)
        .copy()
    )

    st.session_state["df_tmp_class_excel"] = pd.DataFrame(
        columns=st.session_state["schema_excel"].keys(),
    )


def find_next_idx():
    """
    Find next IDX
    """
    if st.session_state["df_class_excel"].empty:
        next_idx = 1
    else:
        list_IDX = (
            st.session_state["df_class_excel"]["IDX"].tolist()
            + st.session_state["df_tmp_class_excel"]["IDX"].tolist()
        )

        # find max in list
        next_idx = (
            pd.Series(list_IDX).str.split("_").str[1].astype(int).max()
            if list_IDX
            else 0
        )
        next_idx = next_idx + 1

    if st.session_state["theme_data"] == "Incidents Railway":
        prefix_idx = "rail_"
        next_idx = f"rail_{next_idx:02}"
    elif st.session_state["theme_data"] == "Incidents Arrest":
        prefix_idx = "arr_"
        next_idx = f"arr_{next_idx:02}"
    # elif st.session_state["theme_data"] == "Incidents Sabotage":
    #     prefix_idx = "sab_"
    #     next_idx = f"sab_{next_idx:02}"

    st.session_state["next_idx"] = next_idx
    st.session_state["prefix_idx"] = prefix_idx


def reinit_row_filt_qualif(ID, IDX):
    """
    Reinit row filt Qualif

    Args:
        ID: ID
        IDX: IDX
    """

    # reinit data qualif by ID
    df_data_qualif = st.session_state["df_qualif"].loc[
        st.session_state["df_qualif"]["ID"] == ID
    ]

    # update cols without IDX
    for col in df_data_qualif.columns:
        if col not in ["IDX"]:
            if col in st.session_state["df_tmp_filt_qualif"].columns:
                st.session_state["df_tmp_filt_qualif"].loc[
                    (st.session_state["df_tmp_filt_qualif"]["ID"] == ID)
                    & (st.session_state["df_tmp_filt_qualif"]["IDX"] == IDX),
                    col,
                ] = df_data_qualif[col].values[0]

    # remove IDX in df_tmp_filt_qualif for ID and IDX
    # Ater all because we need to remove all cols with IDX
    st.session_state["df_tmp_filt_qualif"].loc[
        (st.session_state["df_tmp_filt_qualif"]["ID"] == ID)
        & (st.session_state["df_tmp_filt_qualif"]["IDX"] == IDX),
        "IDX",
    ] = None


def maj_row_excel_to_filt_qualif(ID, IDX, name_df_source):
    """
    Update filt_qualif with data from Excel
    Because data already exists in Excel

    Args:
        ID: ID
        IDX: IDX
        name_df_source: name of dataframe source
    """

    # get data IDX from classify
    df_IDX_exist = st.session_state[name_df_source].loc[
        st.session_state[name_df_source]["IDX"] == IDX
    ]

    # Remove, Rename columns with prefix 'class_' to 'qualif_'
    df_IDX_exist = df_IDX_exist.rename(
        columns=lambda x: x.replace("class_", "qualif_")
    ).drop(columns=["qualif_sources"])

    # add ID
    df_IDX_exist["ID"] = ID

    # Update the existing row with the new data
    for sub_col in df_IDX_exist.columns:
        if sub_col in st.session_state["df_tmp_filt_qualif"].columns:
            st.session_state["df_tmp_filt_qualif"].loc[
                (st.session_state["df_tmp_filt_qualif"]["ID"] == ID),
                sub_col,
            ] = df_IDX_exist[sub_col].values[0]


def upd_ref_input(ID, IDX, col_name):
    """ """

    print("--------------------------------")
    print(f"ID: {ID}")
    print(f"IDX: {IDX}")
    print(f"col Name: {col_name}")
    print(f"Value: {st.session_state[f'REF_{col_name}_{ID}']}")
    print(f"Value: {type(st.session_state[f'REF_{col_name}_{ID}'])}")

    if col_name == "IDX":
        #  Retrieve existing data from the Excel file or the tmp Excel file
        IDX_new = st.session_state[f"REF_{col_name}_{ID}"]

        if IDX_new == "":

            # reinit data qualif by ID
            reinit_row_filt_qualif(ID, IDX)

        # Check if IDX exists in tmp Excel
        if IDX_new in st.session_state["df_tmp_class_excel"]["IDX"].values:
            # Update data qualif
            maj_row_excel_to_filt_qualif(ID, IDX_new, "df_tmp_class_excel")

        if not st.session_state["df_class_excel"].empty:
            if IDX_new in st.session_state["df_class_excel"]["IDX"].values:
                # Update data qualif
                maj_row_excel_to_filt_qualif(ID, IDX_new, "df_class_excel")

    # init new_value
    new_value = st.session_state[f"REF_{col_name}_{ID}"]

    if "date" in col_name:
        new_value = pd.to_datetime(new_value)

    # convert to str if list
    if type(st.session_state[f"REF_{col_name}_{ID}"]) == list:
        if len(st.session_state[f"REF_{col_name}_{ID}"]) > 0:
            new_value = ",".join(st.session_state[f"REF_{col_name}_{ID}"])
        else:
            new_value = ""

    #
    # Update df_tmp_filt_qualif by ID
    if col_name in st.session_state["schema_filter"]:
        st.session_state["df_tmp_filt_qualif"].loc[
            (st.session_state["df_tmp_filt_qualif"]["ID"] == ID),
            col_name,
        ] = new_value

    #
    # Update df_tmp_qualif by ID and IDX condition
    if col_name in st.session_state["schema_qualif"]:
        if IDX == None:
            st.session_state["df_tmp_filt_qualif"].loc[
                (st.session_state["df_tmp_filt_qualif"]["ID"] == ID)
                & (st.session_state["df_tmp_filt_qualif"]["IDX"].isnull()),
                col_name,
            ] = new_value
        else:
            st.session_state["df_tmp_filt_qualif"].loc[
                (st.session_state["df_tmp_filt_qualif"]["ID"] == ID)
                & (st.session_state["df_tmp_filt_qualif"]["IDX"] == IDX),
                col_name,
            ] = new_value

    #
    # Update df_tmp_class_excel by IDX
    if col_name != "IDX":
        col_name_class = col_name.replace("qualif_", "class_")
        if col_name_class in st.session_state["schema_excel"]:
            st.session_state["df_tmp_class_excel"].loc[
                st.session_state["df_tmp_class_excel"]["IDX"] == IDX,
                col_name_class,
            ] = new_value


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

    with st.sidebar:
        if IDX == "" or IDX is None:
            st.error("IDX is required")
            return
        elif not IDX.startswith(st.session_state["prefix_idx"]):
            st.error("IDX not valid")
            return
        elif IDX == st.session_state["prefix_idx"]:
            st.error("IDX not valid")
            return
        # elif not st.session_state["df_class_excel"].empty:
        #     if IDX in st.session_state["df_class_excel"]["IDX"].values:
        #         st.error("IDX already exists")
        #         return

    # get filt_qualif data by ID, select cols for Excel
    df_data_qualif = (
        st.session_state["df_tmp_filt_qualif"]
        .loc[st.session_state["df_tmp_filt_qualif"]["ID"] == ID]
        .rename(
            columns=lambda x: x.replace("qualif", "class").replace(
                "url", "class_sources"
            )
        )
    )[st.session_state["schema_excel"].keys()]

    # add to tmp Excel
    st.session_state["df_tmp_class_excel"] = pd.concat(
        [st.session_state["df_tmp_class_excel"], df_data_qualif]
    ).drop_duplicates(["ID", "IDX"], keep="last")

    # update add_final to True
    st.session_state["df_tmp_filt_qualif"].loc[
        (st.session_state["df_tmp_filt_qualif"]["ID"] == ID)
        & (st.session_state["df_tmp_filt_qualif"]["IDX"] == IDX),
        st.session_state["name_col_add_final"],
    ] = True

    # reinit data qualif by ID
    reinit_row_filt_qualif(ID, IDX)

    # defin next IDX
    find_next_idx()


def prepare_for_save(dict_save):
    """
    Save data in parquet

    Args:
        dict_save: dict with path and dataframe
    """

    ########################
    ## UPDATE FILTER DATA ##
    ########################
    # get cols necessary, according to schema filter
    df_new_filter = st.session_state["df_tmp_filt_qualif"][
        st.session_state["schema_filter"].keys()
    ]

    ########################
    ## UPDATE QUALIF DATA ##
    ########################
    # get cols necessary, according to schema qualif
    df_new_qualif = st.session_state["df_tmp_filt_qualif"][
        st.session_state["schema_qualif"].keys()
    ]

    # get list ID in tmp_filt_qualif AND in tmp Excel
    list_id = st.session_state["df_tmp_class_excel"]["ID"].tolist()

    if len(list_id) > 0:
        ########################
        # remove data present in df_tmp_class_excel
        # Beacause data will be insert in Excel
        # So qualif data need to be MAJ for insert data with qualif_cols filled
        ########################

        # Remove rows in new qualif
        df_new_qualif = df_new_qualif.loc[~df_new_qualif["ID"].isin(list_id)]

        # transform cols of tmp Excel to qualif
        df_excel_to_qualif = st.session_state["df_tmp_class_excel"].rename(
            columns=lambda x: x.replace("class_sources", "url").replace(
                "class_", "qualif_"
            )
        )

        # merge to get date
        df_excel_to_qualif = pd.merge(
            df_excel_to_qualif,
            dict_save["id_date"],
            on=["ID"],
            how="left",
        )

        # add to tmp qualif
        df_new_qualif = pd.concat([df_new_qualif, df_excel_to_qualif]).drop_duplicates(
            ["ID", "IDX"], keep="last"
        )

    #
    # get data, IDX in tmp Excel, for update qualif if IDX already exists in Excel
    df_upd_qualif = st.session_state["df_qualif"].loc[
        st.session_state["df_qualif"]["IDX"].isin(
            st.session_state["df_tmp_class_excel"]["IDX"]
        )
    ]

    if not df_upd_qualif.empty:
        ########################
        # data (IDX) already exists in Excel, it's a update
        # So update all data qualif with IDX updated
        ########################

        # remove all cols qualif_
        df_tmp_new_qualif = df_upd_qualif.drop(
            columns=[col for col in df_upd_qualif.columns if col.startswith("qualif_")]
        )

        # update all cols qualif (merge)
        df_tmp_new_qualif = pd.merge(
            df_tmp_new_qualif,
            df_new_qualif,
            on=["IDX"],
            how="left",
            suffixes=("", "_y"),
        ).drop_duplicates(["ID", "IDX"], keep="last")

        # drop cols _y
        df_tmp_new_qualif = df_tmp_new_qualif.drop(
            columns=[col for col in df_tmp_new_qualif.columns if col.endswith("_y")]
        )

        # merge with new data
        df_new_qualif = pd.concat([df_new_qualif, df_tmp_new_qualif]).drop_duplicates(
            subset=["ID", "IDX"], keep="last"
        )

    ##########################
    ## UPDATE CLASSIFY DATA ##
    ##########################
    # get cols necessary, according to schema Excel
    df_new_classify = st.session_state["df_tmp_class_excel"][
        st.session_state["schema_excel"].keys()
    ]

    # replace qualif_ to class_
    df_upd_qualif = df_upd_qualif.rename(
        columns=lambda x: x.replace("qualif_", "class_").replace("url", "class_sources")
    ).drop(["date", "class_ia"], axis=1)

    # add data already in Excel to new classify
    df_new_classify = pd.concat([df_new_classify, df_upd_qualif])

    # group data by IDX
    df_new_classify = group_url_id_new_classify(df_new_classify)

    # Validation
    validation_checkbox(df_new_filter, df_new_qualif, df_new_classify, list_id)


def group_url_id_new_classify(df):
    """
    Group data by IDX
    If there are multiple same IDX, group all ID and URL

    Args:
        df: DataFrame with new classify data
    """

    # add cols mul_ID, mul_url, regoup all if IDX is muultiple
    df["mul_ID"] = df.groupby("IDX")["ID"].transform(lambda x: ",".join(x))
    df["mul_url"] = df.groupby("IDX")["class_sources"].transform(lambda x: ",".join(x))

    # replace cols
    df = (
        df.drop(columns=["ID", "class_sources"])
        .rename(
            columns={
                "mul_ID": "ID",
                "mul_url": "class_sources",
            }
        )
        .drop_duplicates(["IDX"], keep="first")
    )

    return df


@st.dialog("Validation", width="large")
def validation_checkbox(df_filt, df_qualif, df_class, list_id):
    """
    Validation checkbox
    Display dfs for check berfore save

    Args:
        df_filt: dataframe filter
        df_qualif: dataframe qualif
        df_class: dataframe classify
        list_id: list of ID
    """

    st.write("Filter data")
    st.write(df_filt)

    st.write("Qualif data")
    st.write(df_qualif)

    st.write("IDs Remove from Qualif with IDX null")
    st.write(list_id)

    st.write("Classify data")
    st.write(df_class)

    if st.button("Save", type="primary", use_container_width=True):

        # remove rows in qualif, because data will be insert in Excel
        st.session_state["df_qualif"] = st.session_state["df_qualif"].loc[
            ~(
                st.session_state["df_qualif"]["ID"].isin(list_id)
                & st.session_state["df_qualif"]["IDX"].isnull()
            )
        ]

        # Concat data
        st.session_state["df_filt_src"] = (
            pd.concat([st.session_state["df_filt_src"], df_filt])
            .drop_duplicates(["ID"], keep="last")
            .sort_values(by=["date"])
            .reset_index(drop=True)
        )

        st.session_state["df_qualif"] = (
            pd.concat([st.session_state["df_qualif"], df_qualif])
            .drop_duplicates(["ID", "IDX"], keep="last")
            .sort_values(by=["date"])
            .reset_index(drop=True)
        )

        # find exact name of col date
        col_date = [
            col
            for col in st.session_state["df_tmp_class_excel"].columns
            if "date" in col
        ][0]

        st.session_state["df_class_excel"] = (
            pd.concat([st.session_state["df_class_excel"], df_class])
            .drop_duplicates(["IDX"], keep="last")
            .sort_values(by=[col_date])
            .reset_index(drop=True)
        )

        # update type with schema
        st.session_state["df_class_excel"] = st.session_state["df_class_excel"].astype(
            st.session_state["schema_excel"]
        )
        st.session_state["df_qualif"] = st.session_state["df_qualif"].astype(
            st.session_state["schema_qualif"]
        )

        # Save data
        save_data()

    st.write("")
    st.write("")
    if st.button("Cancel", type="secondary", use_container_width=True):
        st.rerun()


def save_data():
    """
    Save data
    """

    # save data filter
    st.session_state["df_filt_src"].to_parquet(
        f"{st.session_state['path_filter_source']}", index=False
    )

    # save data qualif
    st.session_state["df_qualif"].to_parquet(
        f"{st.session_state['path_qualif']}", index=False
    )

    # save data classify
    st.session_state["df_class_excel"].to_parquet(
        f"{st.session_state['path_classify']}", index=False
    )

    # Reset
    init_state_theme_data()
    init_raw_data()
    init_tmp_data()
    find_next_idx()
    st.rerun()


def apply_basic_filters(df, dict_filter):
    """
    Apply basic filters

    Args:
        df: DataFrame to filter
        dict_filter: Dictionary with filters

    Returns:
        DataFrame filter
    """
    # filter col
    df = df[
        (df[st.session_state.name_col_filter] == dict_filter["filt_col_filter"])
        & (df[st.session_state.name_col_add_final] == dict_filter["filt_col_add_final"])
    ]

    # qualif_ia
    if dict_filter["filt_ia"]:
        # filter only data with qualif_ia = yes
        df = df[df["qualif_ia"] == "yes"]
    else:
        # filter data with qualif_ia = no or null
        df = df[(df["qualif_ia"] == "no") | (df["qualif_ia"].isnull())]

    # Region
    if dict_filter["filt_reg"]:
        df = df[(df["qualif_region"] != "") & (df["qualif_region"].notnull())]

    # Test
    if dict_filter["filt_text"]:
        mask = df["text_translate"].str.contains(
            dict_filter["filt_text"], na=False
        ) | df["text_original"].str.contains(dict_filter["filt_text"], na=False)
        df = df[mask]

    # Date
    if dict_filter["filt_date"]:
        filter_date = pd.to_datetime(dict_filter["filt_date"])
        df = df[df["date"].dt.date >= filter_date.date()]

    return df


# incident_conditions = {
#     "Incidents Railway": {
#         "filt_inc_type": "qualif_incident_type",
#         "filt_dmg_eqp": "qualif_dmg_equipment",
#         "filt_app_law": "qualif_app_laws",
#     },
#     "Incidents Arrest": {
#         "filt_reason": "qualif_arrest_reason",
#         "filt_app_law": "qualif_app_laws",
#     },
# }


def apply_incident_filters(df, dict_filter):
    """
    Apply filters for incidents

    Args:
        df: DataFrame to filter
        dict_filter: Dictionary with filters

    Returns:
        DataFrame filter
    """
    # create query
    query = ""

    for el in dict_filter:
        if dict_filter[el]:
            query += f"({el} != '') & ({el} != 'no') & ({el}.notnull()) & "

    if query:
        query = query[:-3]
        df = df.query(query)

    return df


# ##############################################################################################
# ##############################################################################################
# ##############################################################################################
# ##############################################################################################

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
            # "Incidents Sabotage",
        ],
    )

    st.divider()


if st.session_state["theme_data"] == "":
    st.stop()
elif st.session_state["saved_theme_data"] != st.session_state["theme_data"]:
    st.session_state["saved_theme_data"] = st.session_state["theme_data"]
    init_state_theme_data()
    init_raw_data()
    init_tmp_data()

    # defin next IDX
    find_next_idx()

# st.title(f"Classify Data - {st.session_state['theme_data']}")


with st.expander("Filters", expanded=True):

    col1, col2, col3 = st.columns([0.4, 0.4, 0.4], border=True)

    with col1:
        st.subheader("Globals")

        scol1, scol2 = st.columns([0.5, 0.5])

        with scol1:
            # filter col
            st.toggle(
                "Col Incident Filter",
                key="slt_filt_col_filter",
                value=True,
            )

            # Add final col
            st.toggle(
                "Col Add Final",
                key="slt_filt_col_add_final",
            )

            # text contains
            st.text_input(
                "Text Contains",
                key="slt_filt_txt",
                value="",
            )

        with scol2:
            # toggle claas ia
            st.toggle(
                "Class IA",
                key="slt_filt_ia",
            )

            # toggle class region
            st.toggle(
                "Class Region",
                key="slt_filt_reg",
            )

            # date
            st.date_input("After Date", key="slt_filt_date")

        with col2:
            st.subheader("Specifics")

            if st.session_state["theme_data"] == "Incidents Railway":
                # toggle class incident type
                st.toggle(
                    "Class Incident Type",
                    key="slt_filt_inc_type",
                )

                # toggle class dmg equipment
                st.toggle(
                    "Class Damage Equipment",
                    key="slt_filt_dmg_eqp",
                )

                # toggle class app laws
                st.toggle(
                    "Class Applied Laws",
                    key="slt_filt_app_law",
                )

                # toggle class partisans names
                st.toggle(
                    "Class Partisans Names",
                    key="slt_filt_prtsn_names",
                )

            if st.session_state["theme_data"] == "Incidents Arrest":
                # toggle class reason
                st.toggle(
                    "Class Reason",
                    key="slt_filt_reason",
                )

                # toggle class app laws
                st.toggle(
                    "Class Applied Laws",
                    key="slt_filt_app_law_arr",
                )

    with col3:

        if st.button("Reset Data", type="secondary", use_container_width=True):
            # reload all
            init_tmp_data()

        # Apply Filter on df_filter_qualif
        if st.button("Apply Filter", type="primary", use_container_width=True):

            df_tmp = st.session_state["df_tmp_filt_qualif"].copy()
            dict_filter = {
                "filt_col_filter": st.session_state["slt_filt_col_filter"],
                "filt_col_add_final": st.session_state["slt_filt_col_add_final"],
                "filt_ia": st.session_state["slt_filt_ia"],
                "filt_reg": st.session_state["slt_filt_reg"],
                "filt_text": st.session_state["slt_filt_txt"],
                "filt_date": st.session_state["slt_filt_date"],
            }

            if st.session_state["theme_data"] == "Incidents Railway":
                # add filters
                filters = {
                    "qualif_inc_type": st.session_state["slt_filt_inc_type"],
                    "qualif_dmg_eqp": st.session_state["slt_filt_dmg_eqp"],
                    "qualif_app_laws": st.session_state["slt_filt_app_law"],
                    "qualif_prtsn_names": st.session_state["slt_filt_prtsn_names"],
                }

            elif st.session_state["theme_data"] == "Incidents Arrest":
                # add filters
                filters = {
                    "qualif_arrest_reason": st.session_state["slt_filt_reason"],
                    "qualif_app_laws": st.session_state["slt_filt_app_law_arr"],
                }

            # merge data filter and Qualif
            df_all = pd.merge(
                st.session_state["df_filt_src"].drop(columns=["date", "url"]),
                st.session_state["df_qualif"],
                on=["ID"],
                how="left",
            ).copy()

            # Apply filters
            df_tmp = apply_basic_filters(df_all, dict_filter)
            df_tmp = apply_incident_filters(df_tmp, filters)

            # upd tmp data
            st.session_state["df_tmp_filt_qualif"] = df_tmp.head(HEAD_CLASS)

        # Dislay filter
        # Convert filter values to a DataFrame with 2 columns
        filters_df = pd.DataFrame(
            {
                "Filter": [
                    "Col Incident Filter",
                    "Col Add Final",
                    "Class IA",
                    "Class Region",
                    "Text Contains",
                    "After Date",
                    # "Class Incident Type",
                    # "Class Damage Equipment",
                    # "Class Applied Laws",
                ],
                "Value": [
                    st.session_state["slt_filt_col_filter"],
                    st.session_state["slt_filt_col_add_final"],
                    st.session_state["slt_filt_ia"],
                    st.session_state["slt_filt_reg"],
                    st.session_state["slt_filt_txt"],
                    st.session_state["slt_filt_date"],
                    # st.session_state["slt_filt_inc_type"],
                    # st.session_state["slt_filt_dmg_eqp"],
                    # st.session_state["slt_filt_app_law"],
                ],
            }
        )

        st.dataframe(filters_df)


# with st.expander("Raw data"):
#     st.dataframe(st.session_state.df_filt_src)
#     st.dataframe(st.session_state.df_qualif)
#     st.dataframe(st.session_state.df_class_excel)

with st.expander("Temp data", expanded=True):
    st.dataframe(st.session_state.df_tmp_filt_qualif)
    st.dataframe(st.session_state.df_tmp_class_excel)

with st.expander("Classify Excel"):
    st.dataframe(st.session_state.df_class_excel)


with st.sidebar:
    if st.session_state["theme_data"] == "":
        st.stop()

    st.button(
        "Save Data",
        type="primary",
        use_container_width=True,
        on_click=prepare_for_save,
        args=[
            {
                "path_filter_source": "df_filt_src",
                "path_qualif": "df_qualif",
                "path_classify": "df_class_excel",
                "id_date": st.session_state["df_tmp_filt_qualif"][["ID", "date"]],
            }
        ],
    )

    st.subheader("Variables")

    data = {
        "Next IDX": st.session_state["next_idx"],
        "Remaining Data": st.session_state["df_filt_src"]
        .loc[
            st.session_state["df_filt_src"][st.session_state["name_col_filter"]] == True
        ]
        .shape[0],
        "Filter Source": st.session_state["path_filter_source"].split("/")[-1],
        "Qualif": st.session_state["path_qualif"].split("/")[-1],
        "Classify": st.session_state["path_classify"].split("/")[-1],
        "Col Add Final": st.session_state["name_col_add_final"],
        "Col Filter": st.session_state["name_col_filter"],
    }

    st.table(data)
    st.divider()


# st.write(st.session_state["df_tmp_filt_qualif"].dtypes)

for index, row in st.session_state["df_tmp_filt_qualif"].iterrows():

    text_message = (
        row["text_translate"] if row["text_translate"] != None else row["text_original"]
    )

    ID = row["ID"]
    IDX = row["IDX"]

    col1, col2, col3, col4 = st.columns([0.8, 1.5, 1, 1], border=True)

    with col2:
        st.markdown(f"{ID} - :blue[{row['date']}]")
        st.markdown(f"{row['url']}")

        if row["qualif_region"] != "":
            highlighted_text = text_message.replace(
                row["qualif_region"],
                f":red[{row['qualif_region']}]",
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

    for dict_ref in st.session_state["dict_input"]:
        if dict_ref["name"] in row:
            col = eval(dict_ref["st_col"])

            with col:
                if dict_ref["type"] == "text_input":

                    if dict_ref["name"] == "IDX":
                        st.text_input(
                            dict_ref["label"],
                            value=(
                                row[dict_ref["name"]]
                                if row[dict_ref["name"]]
                                else st.session_state["prefix_idx"]
                            ),
                            key=f"REF_{dict_ref['name']}_{ID}",
                            on_change=upd_ref_input,
                            args=(ID, IDX, dict_ref["name"]),
                        )
                    else:
                        st.text_input(
                            dict_ref["label"],
                            value=(
                                row[dict_ref["name"]] if row[dict_ref["name"]] else ""
                            ),
                            key=f"REF_{dict_ref['name']}_{ID}",
                            on_change=upd_ref_input,
                            args=(ID, IDX, dict_ref["name"]),
                        )

                elif dict_ref["type"] == "checkbox":
                    st.checkbox(
                        dict_ref["label"],
                        value=row[dict_ref["name"]] if row[dict_ref["name"]] else False,
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, IDX, dict_ref["name"]),
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
                        args=(ID, IDX, dict_ref["name"]),
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
                        args=(ID, IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "toggle":
                    st.toggle(
                        dict_ref["label"],
                        value=row[dict_ref["name"]] if row[dict_ref["name"]] else False,
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "number_input":
                    st.number_input(
                        dict_ref["label"],
                        value=(
                            int(row[dict_ref["name"]])
                            if not pd.isnull(row[dict_ref["name"]])
                            else None
                        ),
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, IDX, dict_ref["name"]),
                    )

                elif dict_ref["type"] == "multiselect":
                    st.multiselect(
                        dict_ref["label"],
                        options=[row[dict_ref["name"]], *dict_ref["options"]],
                        default=(
                            [row[dict_ref["name"]]] if row[dict_ref["name"]] else []
                        ),
                        key=f"REF_{dict_ref['name']}_{ID}",
                        on_change=upd_ref_input,
                        args=(ID, IDX, dict_ref["name"]),
                    )

    col1.write("")
    col1.write("")
    col1.write("")
    col1.button(
        "Add New Data",
        type="primary",
        key=f"BTN_add_{ID}",
        use_container_width=True,
        on_click=add_new_data_classify,
        args=(ID, row["date"], row["url"]),
    )

    st.divider()
