import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.states import Completed, Failed


# Variables
from core.config.paths import (
    PATH_DWH_BRONZE,
    PATH_DWH_SILVER_INC_RAILWAY,
    PATH_DWH_BRONZE_INC_RAILWAY,
    PATH_DWH_GOLD,
)

from core.config.variables import SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY

# Utils
from core.common.parquet import read_parquet
from core.libs.duckdb_utils import rewrite_duckdb_table
from core.common.dataframe import reorder_cols, retype_cols


@task(name="Count incidents by column")
def count_incidents_by_column(df, column, type_values):
    """
    Count incidents by a specific column.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    column (str): The column to count incidents by.
    type_values: type of values count

    Returns:
    pd.DataFrame: A DataFrame with the counts.
    """
    df_tmp = df[column].value_counts().reset_index()
    df_tmp.columns = ["metric", "value"]
    df_tmp["libelle"] = type_values
    df_tmp["metric"] = df_tmp["metric"].astype(str)
    return df_tmp


@task(name="Count age group")
def count_age_group(df_age, label, condition):
    total_inc = df_age[condition]["count"].sum()
    data = {"metric": label, "value": total_inc, "libelle": "prtsn_age"}
    return pd.DataFrame(data, index=[0])


@task(name="Total incidents")
def incidents_total(df):
    """
    Total incidents
    """

    df_total = pd.DataFrame()

    columns_to_count = [
        "inc_type",
        "dmg_eqp",
        "region",
        "coll_with",
    ]

    # Total Incidents
    data = {"metric": "Total", "value": df.shape[0], "libelle": "total"}
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Incidents Type
    for col in columns_to_count:
        if col == "dmg_eqp":
            df_count = count_incidents_by_column(
                df[df["coll_with"] != "Human"], col, col
            )
        else:
            df_count = count_incidents_by_column(df, col, col)

        # concat
        df_total = pd.concat([df_total, df_count], ignore_index=True)

    print(df_total)
    # exit()
    # Damage equipment by Collision
    df_tmp = count_incidents_by_column(
        df[df["inc_type"] == "Collision"], "dmg_eqp", "coll_eqp"
    )
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    """
    Incidents Sabotage
    """

    df = df.query("inc_type == 'Sabotage'")

    columns_to_count = [
        # "app_laws",
        "prtsn_grp",
        "prtsn_arr",
    ]

    # incidents for each column
    df_list = [count_incidents_by_column(df, col, col) for col in columns_to_count]

    # Combine all results into a single DataFrame
    df_total = pd.concat([df_total] + df_list, ignore_index=True)
    df_total["metric"] = df_total["metric"].replace(
        {"False": "Sabotage without Arrest", "True": "Sabotage with Arrest"}
    )

    # Sabotage by Region
    df_tmp = count_incidents_by_column(df, "region", "sab_region")
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    # # Partisans Reward
    # df_tmp = count_incidents_by_column(df, "prtsn_rwd", "prtsn_rwd")
    # df_total = pd.concat([df_total, df_tmp], ignore_index=True)
    # df_total["metric"] = df_total["metric"].replace(
    #     {"False": "No Reward", "True": "Reward"}
    # )

    # Sabotage by damaged equipment
    df_tmp = count_incidents_by_column(df, "dmg_eqp", "sabotage")
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    # Damaged equipment by partisans group
    df_tmp = df.groupby(["prtsn_grp", "dmg_eqp"]).size().reset_index()
    df_tmp.columns = ["libelle", "metric", "value"]
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    # get age and name
    df_age_name_laws = df[["prtsn_age", "prtsn_names", "app_laws"]].dropna()
    df_age_name_laws = df_age_name_laws.query(
        "prtsn_age != '' and prtsn_names != '' and app_laws != '' and prtsn_age != '0'"
    )
    df_age_name_laws = df_age_name_laws.drop_duplicates()

    # collect all age (values is string with ,)
    df_age_name_laws = (
        df_age_name_laws.assign(
            prtsn_age=df_age_name_laws["prtsn_age"].str.split(","),
            prtsn_names=df_age_name_laws["prtsn_names"].str.split(","),
            app_laws=df_age_name_laws["app_laws"],
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Count number total of partisans arrested
    data = {
        "metric": "Total Arrested",
        "value": df_age_name_laws["prtsn_names"].count(),
        "libelle": "partisans_arrested",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count Partisans Laws
    df_laws = df_age_name_laws["app_laws"].value_counts().reset_index()
    df_laws.columns = ["metric", "value"]
    df_laws["libelle"] = "app_laws"
    df_total = pd.concat([df_total, df_laws], ignore_index=True)

    # Moy of Partisans Age
    moy = df_age_name_laws["prtsn_age"].astype(int).mean()
    data = {"metric": "Mean", "value": moy, "libelle": "prtsn_age"}
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count partisans age
    df_age = df_age_name_laws["prtsn_age"].value_counts().reset_index()
    df_age.columns = ["prtsn_age", "count"]
    df_age["prtsn_age"] = df_age["prtsn_age"].astype(int)

    # Define age groups and their conditions
    age_groups = [
        ("<18", df_age["prtsn_age"] < 18),
        ("18-30", (df_age["prtsn_age"] >= 18) & (df_age["prtsn_age"] <= 30)),
        ("31-50", (df_age["prtsn_age"] > 30) & (df_age["prtsn_age"] <= 50)),
        (">50", df_age["prtsn_age"] > 50),
    ]

    # Count partisans age for each group
    for metric, condition in age_groups:
        df_total = pd.concat(
            [df_total, count_age_group(df_age, metric, condition)], ignore_index=True
        )

    # Count partisans age
    df_age.columns = ["metric", "value"]
    df_age["metric"] = "age_" + df_age["metric"].astype(str)
    df_age["libelle"] = "prtsn_age"
    df_total = pd.concat([df_total, df_age], ignore_index=True)

    # add last date updated
    last_date_updated = df["date"].max()
    data = {
        "metric": last_date_updated.strftime("%Y-%m-%d"),
        "value": last_date_updated.strftime("%Y-%m-%d"),
        "libelle": "last_date_updated",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # reorder columns
    df_total = df_total[["libelle", "metric", "value"]]

    # remove rows with empty metric
    df_total = df_total.query("metric != ''")

    return df_total


@task(name="Incidents by year")
def incidents_by_year(df):
    """
    Incidents by year
    """

    cols_to_count = ["inc_type", "dmg_eqp", "coll_with"]

    df_tmp = pd.DataFrame()

    for year in df["year"].unique():
        df_year = df[df["year"] == year]

        for col in cols_to_count:
            if col == "dmg_eqp":
                df_count = count_incidents_by_column(
                    df_year[df_year["coll_with"] != "Human"], col, col
                )
            else:
                df_count = count_incidents_by_column(df_year, col, col)

            df_count["year"] = year
            df_tmp = pd.concat([df_tmp, df_count])

        # add total incidents
        data = {
            "year": year,
            "type": "total",
            "label": "Total",
            "total_inc": df_year.shape[0],
        }
        df_tmp = pd.concat([df_tmp, pd.DataFrame(data, index=[0])], ignore_index=True)

    print(df_tmp)

    df_tmp["label"] = df_tmp["label"].astype(str)
    df_tmp = df_tmp[["year", "type", "label", "total_inc"]]

    # remove rows with empty label
    df_tmp = df_tmp.query("label != ''")

    # pivot
    df_tmp = df_tmp.pivot_table(
        index=["type", "label"],
        columns="year",
        values="total_inc",
        fill_value=0,
    ).reset_index()

    # convert columns name to str
    df_tmp.columns = df_tmp.columns.astype(str)

    return df_tmp


@task(name="Incidents by month")
def incidents_by_month(df):
    """
    incidents by month, year
    """

    df_tmp_1 = df.groupby(["year", "month"]).size().reset_index(name="total_inc")

    # count incidents by month, year
    df_tmp_2 = (
        df.groupby(["year", "month", "inc_type"]).size().reset_index(name="total_inc")
    )

    # count damaged equipment by month, year
    df_tmp_3 = (
        df[df["coll_with"] != "Human"]
        .groupby(["year", "month", "dmg_eqp"])
        .size()
        .reset_index(name="total_inc")
    )

    # count collision with by month, year
    df_tmp_4 = (
        df[df["coll_with"] != ""]
        .groupby(["year", "month", "coll_with"])
        .size()
        .reset_index(name="total_inc")
    )

    # count incidents, damaged equipment by month, year
    df_tmp_5 = (
        df.groupby(["year", "month", "inc_type", "dmg_eqp"])
        .size()
        .reset_index(name="total_inc")
    )

    # count incidents, damaged equipment, collision with by month, year
    df_tmp_6 = (
        df[df["coll_with"] != ""]
        .groupby(["year", "month", "inc_type", "dmg_eqp", "coll_with"])
        .size()
        .reset_index(name="total_inc")
    )

    # concat all dataframes
    df_tmp = pd.concat(
        [df_tmp_1, df_tmp_2, df_tmp_3, df_tmp_4, df_tmp_5, df_tmp_6],
        ignore_index=True,
    ).reset_index(drop=True)

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    )

    # reorder columns
    df_tmp = df_tmp[
        ["month_year", "year", "month", "inc_type", "dmg_eqp", "coll_with", "total_inc"]
    ]

    # sort by month_year
    df_tmp = df_tmp.sort_values("month_year")

    # assign name to month
    df_tmp["month"] = df_tmp["month"].replace(
        {
            1: "Jan",
            2: "Feb",
            3: "Mar",
            4: "Apr",
            5: "May",
            6: "Jun",
            7: "Jul",
            8: "Aug",
            9: "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
        }
    )

    return df_tmp


@task(name="Incidents cumul by month")
def incidents_cumul_by_month(df):
    """
    incidents cumul by month
    """

    list_cols = ["inc_type", "dmg_eqp"]

    # Create month_year column
    df["month_year"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str)
    )

    df_fin = pd.DataFrame()

    for col in list_cols:
        if col == "dmg_eqp":
            df = df[df["coll_with"] != "Human"]

        df_tmp = df.groupby(["month_year", col]).size().reset_index(name="total_inc")

        df_tmp["cumul_inc"] = df_tmp.groupby([col])["total_inc"].cumsum()
        df_tmp = df_tmp.drop(columns=["total_inc"]).sort_values(["month_year", col])

        # set of combinations (month_year x col)
        all_months = pd.Series(df["month_year"].unique())
        all_types = df[col].unique()
        index = pd.MultiIndex.from_product(
            [all_months, all_types], names=["month_year", col]
        )

        # Reindex the DataFrame
        df_tmp = (
            df_tmp.set_index(["month_year", col])
            .reindex(index, fill_value=np.nan)
            .reset_index()
        )

        # Fill missing values with 0
        df_tmp["cumul_inc"] = df_tmp.groupby([col])["cumul_inc"].ffill()

        df_fin = pd.concat([df_fin, df_tmp])

    df_fin = df_fin[["month_year", "inc_type", "dmg_eqp", "cumul_inc"]]

    return df_fin


@task(name="Incidents by day, week")
def incidents_by_day_week(df):
    """
    incidents by day, week
    """

    order_days = [
        "Sunday",
        "Saturday",
        "Friday",
        "Thursday",
        "Wednesday",
        "Tuesday",
        "Monday",
    ]

    df_fin = pd.DataFrame()

    for year in df["year"].unique():
        df_year = df[df["year"] == year]

        # add missing days
        start_date = df_year["date"].min()
        end_date = df_year["date"].max()
        all_days = pd.date_range(start_date, end_date, freq="D")
        all_days = pd.DataFrame(all_days, columns=["date"])
        df_year = pd.merge(all_days, df_year, on="date", how="left")

        # add cols day, week
        df_year["day"] = df_year["date"].dt.day_name()
        df_year["week"] = df_year["date"].dt.isocalendar().week

        # pivot table, y (day), x (week)
        df_tmp = df_year.pivot_table(
            index="day",
            columns="week",
            values="inc_type",
            aggfunc="count",
            fill_value=0,
        )

        # add year to columns name
        df_tmp.columns = [f"{year}_{col}" for col in df_tmp.columns]
        df_fin = pd.concat([df_fin, df_tmp], axis=1)

    # sort by order_days
    df_fin = df_fin.reindex(order_days)
    df_fin = df_fin.fillna(0)

    return df_fin


@task(name="Incidents by region")
def incidents_by_region(df):
    """
    incidents by region
    """

    lst_cols = ["inc_type", "dmg_eqp"]

    df_final = pd.DataFrame({"region": df["region"].unique()})
    for col in lst_cols:
        if col == "dmg_eqp":
            df_tmp = (
                df[df["coll_with"] != "Human"]
                .groupby(["region", col])
                .size()
                .reset_index(name="total_inc")
            )
        else:
            df_tmp = df.groupby(["region", col]).size().reset_index(name="total_inc")

        df_tmp = df_tmp.pivot_table(index="region", columns=col, values="total_inc")

        df_final = pd.merge(df_final, df_tmp, on="region", how="outer")

    # Filter for sabotage incidents
    df_sabotage = df[df["inc_type"] == "Sabotage"]

    # Count damaged equipment by region
    df_tmp = (
        df_sabotage.groupby(["region", "dmg_eqp"]).size().reset_index(name="total_inc")
    )

    # pivot table
    df_tmp = df_tmp.pivot_table(index="region", columns="dmg_eqp", values="total_inc")

    # put 'sab_ in front of the column names
    df_tmp.columns = ["sab_" + col for col in df_tmp.columns]

    # merge
    df_final = pd.merge(df_final, df_tmp, on="region", how="outer")

    # Fill missing values with 0
    df_final = df_final.fillna(0)

    return df_final


@task(name="Incidents by incident type and damaged equipment")
def incident_type_damaged_equipment(df):
    """ """

    # remove Collisions with Human
    df = df[df["coll_with"] != "Human"]

    # group by incident type and damaged equipment
    df = df.groupby(["inc_type", "dmg_eqp"]).size().reset_index(name="total_inc")

    return df


@task(name="Prepare data for a Sunburst, Treemap chart")
def incident_type_damaged_equipment_sunburst_treemap(df):
    """
    Incidents by incident type and  damaged equipment for Sunburst, Treemap
    """

    df_inc_type_dmg_eqp = incident_type_damaged_equipment(df)
    ids = []
    parents = []
    labels = []
    values = []

    for incident in df_inc_type_dmg_eqp["inc_type"].unique():
        incident_id = f"inc_{incident}"
        ids.append(incident_id)
        labels.append(incident)
        parents.append("")
        values.append(
            df_inc_type_dmg_eqp[df_inc_type_dmg_eqp["inc_type"] == incident][
                "total_inc"
            ].sum()
        )

        # get the damaged equipment
        df_dmg_eqp = df_inc_type_dmg_eqp[df_inc_type_dmg_eqp["inc_type"] == incident]

        # add the damaged equipment
        for damaged_equipment in df_dmg_eqp["dmg_eqp"].unique():
            damaged_equipment_id = f"{incident_id}_{damaged_equipment}"
            ids.append(damaged_equipment_id)
            labels.append(damaged_equipment)
            parents.append(incident_id)
            values.append(
                df_dmg_eqp[df_dmg_eqp["dmg_eqp"] == damaged_equipment][
                    "total_inc"
                ].sum()
            )

    # create the dataframe
    df_1 = pd.DataFrame(
        {
            "tab": "inc_type",
            "chart": "sun_tree",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    ids = []
    labels = []
    parents = []
    values = []

    # init data
    for incident in df_inc_type_dmg_eqp["dmg_eqp"].unique():
        ids.append(incident)
        labels.append(incident)
        parents.append("")
        values.append(
            df_inc_type_dmg_eqp[df_inc_type_dmg_eqp["dmg_eqp"] == incident][
                "total_inc"
            ].sum()
        )

        # get the damaged equipment
        df_dmg_eqp = df_inc_type_dmg_eqp[df_inc_type_dmg_eqp["dmg_eqp"] == incident]

        # add the damaged equipment
        for damaged_equipment in df_dmg_eqp["inc_type"].unique():
            ids.append(f"{incident}_{damaged_equipment}")
            labels.append(damaged_equipment)
            parents.append(incident)
            values.append(
                df_dmg_eqp[df_dmg_eqp["inc_type"] == damaged_equipment][
                    "total_inc"
                ].sum()
            )

    # create the dataframe
    df_2 = pd.DataFrame(
        {
            "tab": "dmg_eqp",
            "chart": "sun_tree",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # Damaged equipment by partisans group for Sunburst, Treemap
    df_tmp = (
        df.query("inc_type == 'Sabotage' and prtsn_grp != ''")
        .groupby(["dmg_eqp", "prtsn_grp"])
        .size()
        .reset_index(name="total_inc")
    )
    # ----------------------------------------------

    # print("df_tmp before sun_tree\n", df_tmp)

    ids = []
    labels = []
    parents = []
    values = []

    for equip in df_tmp["dmg_eqp"].unique():
        equip_id = f"dmg_eqp_{equip}"
        ids.append(equip_id)
        labels.append(equip)
        parents.append("")
        values.append(df_tmp[df_tmp["dmg_eqp"] == equip]["total_inc"].sum())

        # get the partisans group
        df_prtsn_grp = df_tmp[df_tmp["dmg_eqp"] == equip]

        # add the partisans group
        for group in df_prtsn_grp["prtsn_grp"].unique():
            group_id = f"{equip_id}_{group}"
            ids.append(group_id)
            labels.append(group)
            parents.append(equip_id)
            values.append(
                df_prtsn_grp[df_prtsn_grp["prtsn_grp"] == group]["total_inc"].sum()
            )

    # create the dataframe
    df_6 = pd.DataFrame(
        {
            "tab": "dmg_eqp_prtsn_grp",
            "chart": "sun_tree",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # print("df_6\n", df_6)

    # ----------------------------------------------

    ids = []
    labels = []
    parents = []
    values = []

    for group in df_tmp["prtsn_grp"].unique():
        group_id = f"prtsn_grp_{group}"
        ids.append(group_id)
        labels.append(group)
        parents.append("")
        values.append(df_tmp[df_tmp["prtsn_grp"] == group]["total_inc"].sum())

        # get the damaged equipment
        df_dmg_eqp = df_tmp[df_tmp["prtsn_grp"] == group]

        # add the damaged equipment
        for equip in df_dmg_eqp["dmg_eqp"].unique():
            equip_id = f"{group_id}_{equip}"
            ids.append(equip_id)
            labels.append(equip)
            parents.append(group_id)
            values.append(df_dmg_eqp[df_dmg_eqp["dmg_eqp"] == equip]["total_inc"].sum())

    # create the dataframe
    df_7 = pd.DataFrame(
        {
            "tab": "prtsn_grp_dmg_eqp",
            "chart": "sun_tree",
            "id": ids,
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # ----------------------------------------------

    source = []
    target = []
    value = []

    # Create a list of unique labels
    labels = list(
        set(
            list(df_inc_type_dmg_eqp["inc_type"]) + list(df_inc_type_dmg_eqp["dmg_eqp"])
        )
    )

    # Create a mapping dictionary
    label_to_index = {label: i for i, label in enumerate(labels)}

    # Create source, target, and value lists
    source = [label_to_index[label] for label in df_inc_type_dmg_eqp["inc_type"]]
    target = [label_to_index[label] for label in df_inc_type_dmg_eqp["dmg_eqp"]]
    value = df_inc_type_dmg_eqp["total_inc"]

    # add missing values in labels
    labels = labels + [""] * (len(source) - len(labels))

    # create the dataframe
    df_3 = pd.DataFrame(
        {
            "tab": "inc_type",
            "chart": "sankey",
            "label": labels,
            "source": source,
            "target": target,
            "value": value,
        }
    )

    # data for sankey diagram
    # Create a list of unique labels

    source = []
    target = []
    value = []

    labels = list(
        set(
            list(df_inc_type_dmg_eqp["dmg_eqp"]) + list(df_inc_type_dmg_eqp["inc_type"])
        )
    )

    # Create a mapping dictionary
    label_to_index = {label: i for i, label in enumerate(labels)}

    # Create source, target, and value lists
    source = [label_to_index[label] for label in df_inc_type_dmg_eqp["dmg_eqp"]]
    target = [label_to_index[label] for label in df_inc_type_dmg_eqp["inc_type"]]
    value = df_inc_type_dmg_eqp["total_inc"]

    # add missing values in labels
    labels = labels + [""] * (len(source) - len(labels))

    # create the dataframe
    df_4 = pd.DataFrame(
        {
            "tab": "dmg_eqp",
            "chart": "sankey",
            "label": labels,
            "source": source,
            "target": target,
            "value": value,
        }
    )

    # # Sankey Number of Apliicable Laws by Partisans Reward by Partisans Arrest
    # source = []
    # target = []
    # value = []

    # # df_tmp = df.query("partisans_arrest == True")

    # df_tmp = (
    #     df.query("prtsn_arr == True")
    #     .groupby(["app_laws", "prtsn_rwd"])
    #     .size()
    #     .reset_index(name="total_inc")
    # )

    # # rename
    # df_tmp["prtsn_rwd"] = df_tmp["prtsn_rwd"].replace(
    #     {True: "Reward", False: "No Reward"}
    # )
    # df_tmp["prtsn_arr"] = "Arrested"

    # # Create a list of unique labels
    # labels = list(
    #     set(["Arrested"] + list(df_tmp["prtsn_rwd"]) + list(df_tmp["app_laws"]))
    # )

    # # Create a mapping dictionary
    # label_to_index = {label: i for i, label in enumerate(labels)}

    # # Create source, target, and value lists
    # source = (
    #     [
    #         label_to_index["Reward"]
    #         for _ in range(len(df_tmp.query("prtsn_rwd == 'Reward'")))
    #     ]
    #     + [
    #         label_to_index["No Reward"]
    #         for _ in range(len(df_tmp.query("prtsn_rwd == 'No Reward'")))
    #     ]
    #     + [label_to_index["Arrested"] for _ in range(len(df_tmp["prtsn_rwd"].unique()))]
    # )

    # # get index of app_laws where prtsn_rwd == 'Reward'
    # target = (
    #     [
    #         label_to_index[laws]
    #         for laws in df_tmp.query("prtsn_rwd == 'Reward'")["app_laws"]
    #     ]
    #     + [
    #         label_to_index[laws]
    #         for laws in df_tmp.query("prtsn_rwd == 'No Reward'")["app_laws"]
    #     ]
    #     + [label_to_index["Reward"], label_to_index["No Reward"]]
    # )

    # # browse source and target
    # for i in range(len(source) - 2):
    #     value.append(
    #         df_tmp.query(
    #             f"prtsn_rwd == '{labels[source[i]]}' and app_laws == '{labels[target[i]]}'"
    #         )["total_inc"].values[0]
    #     )

    # value.append(
    #     df_tmp.query(f"prtsn_rwd == '{labels[target[-2]]}'")["total_inc"].sum()
    # )
    # value.append(
    #     df_tmp.query(f"prtsn_rwd == '{labels[target[-1]]}'")["total_inc"].sum()
    # )

    # # add missing values in labels
    # labels = labels + [""] * (len(source) - len(labels))

    # df_5 = pd.DataFrame(
    #     data={
    #         "tab": "prtsn_rwd",
    #         "chart": "sankey",
    #         "label": labels,
    #         "source": source,
    #         "target": target,
    #         "value": value,
    #     }
    # )

    # df_final = pd.concat([df_1, df_2, df_3, df_4, df_5, df_6, df_7], ignore_index=True)
    df_final = pd.concat([df_1, df_2, df_3, df_4, df_6, df_7], ignore_index=True)

    return df_final


@task(name="Get all incidents of sabotage")
def sabotage_by_partisans_group(df):
    """
    Get all incidents of sabotage
    """

    df = df[df["inc_type"] == "Sabotage"]
    df = df[["date", "prtsn_grp"]]

    return df


@task(name="Prepare data for a heatmap chart")
def applicable_laws_partisans_age(df):
    """
    Prepare data for a heatmap chart
    For each age unique, count the number of applicable laws
    """

    # Get unique age and names (age != "")
    df_age_name = (
        df.query("prtsn_age != ''")
        .assign(
            prtsn_age=df["prtsn_age"].str.split(","),
            prtsn_names=df["prtsn_names"].str.split(","),
        )
        .explode(["prtsn_age", "prtsn_names"])
        .reset_index(drop=True)
    )

    # Create a DataFrame to store the results
    df_final = pd.DataFrame()

    # Count the number of applicable laws
    df_tmp = (
        df_age_name.groupby(["prtsn_age", "app_laws"])
        .size()
        .reset_index(name="total_inc")
    )

    # remove rows "Law not indicated"
    df_tmp = df_tmp.query("app_laws != ''")

    # Pivot table
    df_final = df_tmp.pivot_table(
        index="prtsn_age",
        columns="app_laws",
        values="total_inc",
        fill_value=0,
    )

    return df_final


@task(name="Prepare data for a wordcloud chart")
def wordcloud(df):
    """
    Prepare data for a wordcloud chart
    join all words in specific columns
    """

    list_cols = [
        "region",
        "inc_type",
        "dmg_eqp",
        "prtsn_grp",
        "app_laws",
    ]

    # join all words in specific columns
    df_final = pd.DataFrame()
    df_final["text"] = df[list_cols].apply(lambda x: ", ".join(x.dropna()), axis=1)

    # decompose text (split by ,)
    df_final = (
        df_final["text"]
        .str.split(", ", expand=True)
        .stack()
        .reset_index()
        .drop(columns=["level_0", "level_1"])
    )
    df_final.columns = ["text"]

    # count words
    df_final = df_final["text"].value_counts().reset_index()
    df_final.columns = ["text", "total_inc"]

    return df_final


# ---
#
#
#
#
#
#
#
#
@task(name="DMT KPIs")
def dmt_kpis(df):
    """
    Compute Dmt KPIs:
    - Last date updated
    - Total incidents
    - Total Sabotage
    - Total Arrested Partisans
    """
    if df.empty:
        return pd.DataFrame(columns=["libelle", "metric", "value"])

    # collect all age (values is string with ,)
    df_names = (
        df[["prtsn_age", "prtsn_names", "app_laws"]]
        .dropna()
        .query(
            "prtsn_age != '' and prtsn_names != '' and app_laws != '' and prtsn_age != '0'"
        )
    ).drop_duplicates()

    df_names = (
        df_names.assign(prtsn_names=df["prtsn_names"].str.split(","))
        .explode(["prtsn_names"])
        .reset_index(drop=True)
    )

    last_date = df["date"].max()
    kpis = [
        {
            "libelle": "kpi1",
            "metric": "last_date_updated",
            "value": last_date.strftime("%Y-%m-%d"),
        },
        {
            "libelle": "kpi2",
            "metric": "total_incidents",
            "value": len(df),
        },
        {
            "libelle": "kpi3",
            "metric": "total_sabotage",
            "value": (df["inc_type"] == "Sabotage").sum(),
        },
        {
            "libelle": "kpi4",
            "metric": "total_arrested_partisans",
            "value": df_names["prtsn_names"].count(),
        },
    ]

    return pd.DataFrame(kpis)


@task(name="Overview global incidents")
def overview_global_incidents(df):
    """
    Overview global incidents:
    - Total incidents by year
    - Total incidents by month
    - Total by incident type
    - Total by damaged equipment
    - Total by Region
    - Total by Collisions with
    - Total Sabotage by year
    - Total Sabotage by Partisans group
    - Total Damaged equipment for incident Sabotage
    - Total Sabotage by Region
    """
    if df.empty:
        return pd.DataFrame(columns=["metric", "libelle", "value"])

    results = []

    # Year
    year = df.groupby("year").size().reset_index(name="value")
    year.columns = ["metric", "value"]
    year["libelle"] = "year"
    results.append(year)

    # Month (avec format MM-YYYY)
    month = df.groupby(["year", "month"]).size().reset_index(name="value")
    month["metric"] = (
        month["month"].apply(lambda x: f"{x:02d}") + "-" + month["year"].astype(str)
    )
    month["libelle"] = "month"
    results.append(month[["metric", "libelle", "value"]])

    # Incident type
    inc_type = df.groupby("inc_type").size().reset_index(name="value")
    inc_type.columns = ["metric", "value"]
    inc_type["libelle"] = "inc_type"
    results.append(inc_type)

    # Damaged equipment (filtre Human)
    dmg_eqp = (
        df[df["coll_with"] != "Human"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp.columns = ["metric", "value"]
    dmg_eqp["libelle"] = "dmg_eqp"
    results.append(dmg_eqp)

    # Region
    region = df.groupby("region").size().reset_index(name="value")
    region.columns = ["metric", "value"]
    region["libelle"] = "region"
    results.append(region)

    # Collisions with
    coll_with = (
        df[df["inc_type"] == "Collision"]
        .groupby("coll_with")
        .size()
        .reset_index(name="value")
    )
    coll_with.columns = ["metric", "value"]
    coll_with["libelle"] = "coll_with"
    results.append(coll_with)

    # Damaged equipment for incident Collisions
    dmg_eqp_coll = (
        df[df["inc_type"] == "Collision"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp_coll.columns = ["metric", "value"]
    dmg_eqp_coll["libelle"] = "dmg_eqp_coll"
    results.append(dmg_eqp_coll)

    # Sabotage by year
    sabotage_year = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("year")
        .size()
        .reset_index(name="value")
    )
    sabotage_year.columns = ["metric", "value"]
    sabotage_year["libelle"] = "sabotage_year"
    results.append(sabotage_year)

    # Sabotage by Partisans group
    sabotage_prtsn_grp = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("prtsn_grp")
        .size()
        .reset_index(name="value")
    )
    sabotage_prtsn_grp.columns = ["metric", "value"]
    sabotage_prtsn_grp["libelle"] = "prtsn_grp"
    results.append(sabotage_prtsn_grp)

    # Damaged equipment for incident Sabotage
    dmg_eqp_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("dmg_eqp")
        .size()
        .reset_index(name="value")
    )
    dmg_eqp_sabotage.columns = ["metric", "value"]
    dmg_eqp_sabotage["libelle"] = "dmg_eqp_sabotage"
    results.append(dmg_eqp_sabotage)

    # Sabotage by Region
    sabotage_region = (
        df[df["inc_type"] == "Sabotage"]
        .groupby("region")
        .size()
        .reset_index(name="value")
    )
    sabotage_region.columns = ["metric", "value"]
    sabotage_region["libelle"] = "region_sabotage"
    results.append(sabotage_region)

    return pd.concat(results, ignore_index=True)


@task(name="DMT Incidents by month")
def dmt_incidents_by_month(df):
    """
    DMT Incidents by month

    - Total incidents by month and year
    """

    df_tmp = df.groupby(["year", "month"]).size().reset_index(name="total_inc")
    df_tmp["libelle"] = df_tmp["year"].astype(str)
    df_tmp["metric"] = df_tmp["month"].apply(lambda x: f"{x:02d}")
    df_tmp = df_tmp[["libelle", "metric", "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by day")
def dmt_incidents_by_day(df):
    """
    DMT Incidents by day

    - Total incidents by day
    """

    df_tmp = df.groupby("date").size().reset_index(name="total_inc")
    df_tmp["libelle"] = df_tmp["date"].dt.strftime("%Y-%m-%d")
    df_tmp["metric"] = df_tmp["date"].dt.strftime("%Y-%m-%d")
    df_tmp = df_tmp[["libelle", "metric", "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by region")
def dmt_incidents_by_region(df):
    """
    DMT Incidents by region

    - Total incidents by region
    """

    df_tmp = df.groupby("region").size().reset_index(name="total_inc")
    df_tmp["parent"] = "Russia"
    df_tmp["label"] = df_tmp["region"]
    df_tmp = df_tmp[["parent", "label", "total_inc"]]
    df_tmp.columns = ["parent", "label", "value"]

    # remove rows with empty label
    df_tmp = df_tmp.query("label != ''")

    return df_tmp


@task(name="DMT Incidents Metric month/year")
def dmt_metric_by_month(df, metric):
    """
    DMT Incidents Metric month/year

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type', 'dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value
    """

    df_tmp = (
        df.groupby(["year", "month", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    ).dt.strftime("%Y-%m")

    # select cols
    df_tmp = df_tmp[["month_year", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by year")
def dmt_metric_by_year(df, metric):
    """
    DMT Incidents by metric and by year

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type', 'dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    df_tmp = (
        df.groupby(["year", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year"])
    )

    # select cols
    df_tmp = df_tmp[["year", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by region")
def dmt_metric_by_region(df, metric):
    """
    DMT Incidents by metric and by region

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type','dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value
    """

    df_tmp = (
        df.groupby(["region", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["region"])
    )

    # select cols
    df_tmp = df_tmp[["region", metric, "total_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by metric and by month/year (cumulative)")
def dmt_metric_by_month_cumul(df, metric):
    """
    DMT Incidents by metric and by month/year (cumulative)

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        metric (str): The column name to group by (e.g., 'inc_type','dmg_eqp', etc.).

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    df_tmp = (
        df.groupby(["year", "month", metric])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    )

    # compute cumulative sum by incident type
    df_tmp["cumul_inc"] = df_tmp.groupby(metric)["total_inc"].cumsum()

    # Create a complete date range from min to max month
    date_min = df_tmp["month_year"].min()
    date_max = df_tmp["month_year"].max()
    date_range = pd.date_range(start=date_min, end=date_max, freq="MS")

    # Get unique incident types
    incident_types = df_tmp[metric].unique()

    # Create a complete grid: all incident types × all months
    complete_grid = pd.MultiIndex.from_product(
        [incident_types, date_range], names=[metric, "month_year"]
    ).to_frame(index=False)

    # Merge with existing data (right join to keep all months)
    df_tmp = df_tmp[[metric, "month_year", "cumul_inc"]].merge(
        complete_grid, on=[metric, "month_year"], how="right"
    )

    # Sort by incident type and date
    df_tmp = df_tmp.sort_values([metric, "month_year"])

    # Forward fill missing values by incident type (fill with last known cumulative)
    df_tmp["cumul_inc"] = df_tmp.groupby(metric)["cumul_inc"].ffill()

    # select cols
    df_tmp = df_tmp[[metric, "month_year", "cumul_inc"]]
    df_tmp.columns = ["libelle", "metric", "value"]

    return df_tmp


@task(name="DMT Incidents by incident type and by damaged equipment")
def dmt_incidents_on_2_lvl(df, lvl_1, lvl_2):
    """
    DMT Incidents on 2 levels:
        example: incident type and damaged equipment

    Args:
        df (pd.DataFrame): DataFrame containing incidents
        lvl_1 (str): The first level column name (e.g., 'inc_type').
        lvl_2 (str): The second level column name (e.g., 'dmg_eqp').

    Returns:
        pd.DataFrame: DataFrame with columns 'parent', 'label', and 'value'
    """

    parents = []
    labels = []
    values = []

    for x in df[lvl_1].unique():
        # get the damaged equipment
        df_tmp = df[df[lvl_1] == x]

        # add the damaged equipment
        for y in df_tmp[lvl_2].unique():
            labels.append(y)
            parents.append(x)
            values.append(df_tmp[df_tmp[lvl_2] == y].shape[0])

    # create the dataframe
    df_final = pd.DataFrame(
        {
            "parent": parents,
            "label": labels,
            "value": values,
        }
    )

    # Update types
    dict_types = {
        "parent": "string",
        "label": "string",
        "value": "int64",
    }

    # reorder and retype columns
    df_final = reorder_cols(df_final, dict_types)
    df_final = retype_cols(df_final, dict_types)

    return df_final


@task(name="DMT Incidents Total vs Sabotage over time")
def dmt_incidents_total_vs_sabotage(df):
    """
    DMT Incidents Total vs Sabotage over time (month/year)

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Total incidents by month/year
    df_total = (
        df.groupby(["year", "month"])
        .size()
        .reset_index(name="total_inc")
        .sort_values(["year", "month"])
    )
    df_total["month_year"] = pd.to_datetime(
        df_total["year"].astype(str) + "-" + df_total["month"].astype(str)
    ).dt.strftime("%Y-%m")
    df_total = df_total[["month_year", "total_inc"]]
    df_total.columns = ["libelle", "value"]
    df_total["metric"] = "Incidents"

    # Sabotage incidents by month/year
    df_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby(["year", "month"])
        .size()
        .reset_index(name="sabotage_inc")
        .sort_values(["year", "month"])
    )
    df_sabotage["month_year"] = pd.to_datetime(
        df_sabotage["year"].astype(str) + "-" + df_sabotage["month"].astype(str)
    ).dt.strftime("%Y-%m")
    df_sabotage = df_sabotage[["month_year", "sabotage_inc"]]
    df_sabotage.columns = ["libelle", "value"]
    df_sabotage["metric"] = "Sabotage"

    # add missing months for sabotage (if no sabotage in a month, value = 0)
    df_sabotage = (
        df_total[["libelle"]]
        .merge(df_sabotage, on="libelle", how="left")
        .fillna({"value": 0, "metric": "Sabotage"})
    ).sort_values("libelle")

    # Merge total and sabotage
    df_final = pd.concat([df_sabotage, df_total], ignore_index=True)

    return df_final


@task(name="DMT Incidents Sabotage by Partisans group over time")
def dmt_incidents_sabotage_by_prtsn_grp_over_time(df):
    """
    DMT Incidents Sabotage by Partisans group over time (day)

    Args:
        df (pd.DataFrame): DataFrame containing incidents

    Returns:
        pd.DataFrame: DataFrame with columns 'libelle', 'metric', and 'value'
    """

    # Sabotage incidents by day and partisans group
    df_sabotage = (
        df[df["inc_type"] == "Sabotage"]
        .groupby(["date", "prtsn_grp"])
        .size()
        .reset_index(name="sabotage_inc")
        .sort_values(["date", "prtsn_grp"])
    )
    df_sabotage["libelle"] = df_sabotage["date"].dt.strftime("%Y-%m-%d")
    df_sabotage = df_sabotage[["libelle", "prtsn_grp", "sabotage_inc"]]
    df_sabotage.columns = ["libelle", "metric", "value"]

    return df_sabotage


@flow(
    name="DWH Subflow Datamarts Incidents Railway",
    flow_run_name="dwh-subflow-dmt-incident-railway",
    log_prints=True,
)
def subflow_dmt_incident_railway():
    """
    Datamarts Incidents Railway
    """

    # read data from transform
    # df = read_parquet(PATH_DWH_SILVER_INC_RAILWAY, "incidents_railway")
    df = read_parquet(PATH_DWH_BRONZE_INC_RAILWAY, "incidents_railway")

    schema = SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY

    if df.empty:
        return Failed(message="Data is empty")

    # KPIs
    df_tmp = dmt_kpis(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_kpis", PATH_DWH_GOLD)

    # Incidents total
    df_tmp = overview_global_incidents(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_global", PATH_DWH_GOLD)

    # Incidents by month
    df_tmp = dmt_incidents_by_month(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_month", PATH_DWH_GOLD)

    # Incidents by day
    df_tmp = dmt_incidents_by_day(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_day", PATH_DWH_GOLD)

    # Incidents by region
    df_tmp = dmt_incidents_by_region(df)
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_by_region", PATH_DWH_GOLD)

    #
    #
    # Incidents Types
    # Incidents by incident type and by month/year
    df_tmp = dmt_metric_by_month(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_month", PATH_DWH_GOLD)

    # Incidents by incident type and by year
    df_tmp = dmt_metric_by_year(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_year", PATH_DWH_GOLD)

    # Incidents by incident type and by month/year (cumulative)
    df_tmp = dmt_metric_by_month_cumul(df, "inc_type")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_types_by_month_cumul", PATH_DWH_GOLD
    )

    # Incidents by incident type and by damaged equipment
    df_tmp = dmt_incidents_on_2_lvl(df, "inc_type", "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_types_by_damaged_equipment", PATH_DWH_GOLD
    )

    # Incidents by incident type and by region
    df_tmp = dmt_metric_by_region(df, "inc_type")
    rewrite_duckdb_table(df_tmp, schema, "dmt_incidents_types_by_region", PATH_DWH_GOLD)

    #
    #
    #
    # Damaged Equipment
    # DMT Incidents by damaged equipment and by month/year
    df_tmp = dmt_metric_by_month(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_month", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by year
    df_tmp = dmt_metric_by_year(df, "dmg_eqp")
    rewrite_duckdb_table(df_tmp, schema, "dmt_damaged_equipment_by_year", PATH_DWH_GOLD)

    # DMT Incidents by damaged equipment and by month/year (cumulative)
    df_tmp = dmt_metric_by_month_cumul(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_month_cumul", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by region
    df_tmp = dmt_metric_by_region(df, "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_region", PATH_DWH_GOLD
    )

    # DMT Incidents by damaged equipment and by incident type
    df_tmp = dmt_incidents_on_2_lvl(df, "dmg_eqp", "inc_type")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_damaged_equipment_by_incident_type", PATH_DWH_GOLD
    )

    #
    #
    #
    # Collisions
    # DMT Incidents Type = Collisions by month/year
    # df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "coll_with")
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "coll_with")
    rewrite_duckdb_table(df_tmp, schema, "dmt_collisions_by_month", PATH_DWH_GOLD)

    # DMT Incidents Type = Collisions by damaged equipment and by month/year
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Collision'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp,
        schema,
        "dmt_collisions_by_damaged_equipment_by_month",
        PATH_DWH_GOLD,
    )

    #
    #
    # Sabotage
    # Sabotages by month/year
    df_tmp = dmt_incidents_by_month(df.query("inc_type == 'Sabotage'"))
    rewrite_duckdb_table(df_tmp, schema, "dmt_sabotage_by_month", PATH_DWH_GOLD)

    # Incidents Total vs Sabotage over time (month/year)
    df_tmp = dmt_incidents_total_vs_sabotage(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_incidents_total_vs_sabotage", PATH_DWH_GOLD
    )

    # Sabotage by Partisans group over time (day)
    df_tmp = dmt_incidents_sabotage_by_prtsn_grp_over_time(df)
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_prtsn_grp_over_time", PATH_DWH_GOLD
    )

    # Sabotage by Partisans group and by damaged equipment
    df_tmp = dmt_incidents_on_2_lvl(
        df.query("inc_type == 'Sabotage'"), "prtsn_grp", "dmg_eqp"
    )
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_prtsn_grp_by_damaged_equipment", PATH_DWH_GOLD
    )

    # Sabotage by damaged equipment and by Partisans group
    df_tmp = dmt_incidents_on_2_lvl(
        df.query("inc_type == 'Sabotage'"), "dmg_eqp", "prtsn_grp"
    )
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_damaged_equipment_by_prtsn_grp", PATH_DWH_GOLD
    )

    # Damaged equipment in Sabotage by month/year
    df_tmp = dmt_metric_by_month(df.query("inc_type == 'Sabotage'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_damaged_equipment_by_month", PATH_DWH_GOLD
    )

    # Sabotage by Region and by damaged equipment
    df_tmp = dmt_metric_by_region(df.query("inc_type == 'Sabotage'"), "dmg_eqp")
    rewrite_duckdb_table(
        df_tmp, schema, "dmt_sabotage_by_region_by_damaged_equipment", PATH_DWH_GOLD
    )

    # d
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #

    # # Incidents by year
    # df_tmp = incidents_by_year(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_by_year", PATH_DWH_GOLD)
    #
    # # Incidents by month
    # df_tmp = incidents_by_month(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_by_month", PATH_DWH_GOLD)
    #
    # # Incidents by day, week
    # df_tmp = incidents_by_day_week(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_by_day_week", PATH_DWH_GOLD)
    #
    # # Incidents by region
    # df_tmp = incidents_by_region(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_by_region", PATH_DWH_GOLD)
    #
    # # count occurrences of incident type and damaged equipment
    # df_tmp = incident_type_damaged_equipment(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_type_dmg_eqp", PATH_DWH_GOLD)
    #
    # # Incidents by incident type and damaged equipment
    # # For Graphs: Sunburst, Treemap
    # df_tmp = incident_type_damaged_equipment_sunburst_treemap(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_type_dmg_eqp_sun_tree", PATH_DWH_GOLD)
    #
    # # Sabotage by partisans group
    # df_tmp = sabotage_by_partisans_group(df)
    # rewrite_duckdb_table(df_tmp, schema, "sabotage_by_prtsn_grp", PATH_DWH_GOLD)
    #
    # # Applicable laws by partisans age
    # df_tmp = applicable_laws_partisans_age(df)
    # rewrite_duckdb_table(df_tmp, schema, "app_laws_prtsn_age", PATH_DWH_GOLD)
    #
    # # Wordcloud
    # df_tmp = wordcloud(df)
    # rewrite_duckdb_table(df_tmp, schema, "wordcloud", PATH_DWH_GOLD)
    #
    # # # Incidents cumul by month
    # df_tmp = incidents_cumul_by_month(df)
    # rewrite_duckdb_table(df_tmp, schema, "inc_cumul_by_month", PATH_DWH_GOLD)

    return Completed(message="All DataMarts Incidents Railway Completed")
