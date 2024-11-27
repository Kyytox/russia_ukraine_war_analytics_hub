import os
import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.states import Failed


# Variables
from core.utils.variables import (
    path_dw_transform,
    path_dm_incidents_railway,
)

# Utils
from core.libs.utils import read_data, save_data


@task(name="Count incidents by column")
def count_incidents_by_column(df, column, incident_type_filter=None):
    """
    Count incidents by a specific column.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    column (str): The column to count incidents by.
    incident_type_filter (str, optional): Filter the DataFrame by this incident type before counting.

    Returns:
    pd.DataFrame: A DataFrame with the counts.
    """
    df_tmp = df[column].value_counts().reset_index()
    df_tmp.columns = ["label", "total_inc"]
    df_tmp["type"] = column
    df_tmp["label"] = df_tmp["label"].astype(str)
    return df_tmp


@task(name="Total incidents")
def incidents_total(df):
    """
    Total incidents
    """
    columns_to_count = [
        "inc_type",
        "dmg_eqp",
        "region",
    ]

    # Count incidents for each column
    df_list = [count_incidents_by_column(df, col) for col in columns_to_count]
    df_total = pd.concat(df_list, ignore_index=True)

    # add total incidents
    data = {"label": "Total", "total_inc": df.shape[0], "type": "total"}
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    df = df.query("inc_type == 'Sabotage'")

    columns_to_count = [
        "app_laws",
        "prtsn_grp",
        "prtsn_arr",
    ]

    # Count incidents for each column
    df_list = [count_incidents_by_column(df, col) for col in columns_to_count]

    # Combine all results into a single DataFrame
    # df_total = pd.concat(df_list, ignore_index=True)
    df_total = pd.concat([df_total] + df_list, ignore_index=True)
    df_total["label"] = df_total["label"].replace(
        {"False": "No Arrested", "True": "Arrested"}
    )

    df_tmp = df[df["prtsn_arr"] == True].value_counts("prtsn_rwd").reset_index()
    df_tmp.columns = ["label", "total_inc"]
    df_tmp["type"] = "prtsn_rwd"
    df_tmp["label"] = df_tmp["label"].astype(str)
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)
    df_total["label"] = df_total["label"].replace(
        {"False": "No Reward", "True": "Reward"}
    )

    # count Sabotage by damaged equipment
    df_tmp = df["dmg_eqp"].value_counts().reset_index()
    df_tmp.columns = ["label", "total_inc"]
    df_tmp["type"] = "sabotage"
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    # count damaged equipment by partisans group
    df_tmp = df.groupby(["prtsn_grp", "dmg_eqp"]).size().reset_index()
    df_tmp.columns = ["type", "label", "total_inc"]
    df_total = pd.concat([df_total, df_tmp], ignore_index=True)

    # collect all age (values is string with ,)
    # age = df["prtsn_age"].str.split(",").explode().astype(float).dropna()
    age = (
        df["prtsn_age"]
        .str.split(",")
        .explode()
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .astype(float)
    )

    # Moy of Partisans Age
    moy = age.mean()
    data = {"label": "Mean", "total_inc": moy, "type": "prtsn_age"}
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count partisans age
    df_age = pd.DataFrame(age.value_counts()).reset_index()
    df_age["prtsn_age"] = df_age["prtsn_age"].astype(int)

    # Count partisans age <18 (use df_age)
    data = {
        "label": "<18",
        "total_inc": df_age[df_age["prtsn_age"] < 18]["count"].sum(),
        "type": "prtsn_age",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # dot the same for the other age groups
    # Count partisans age >18<30
    data = {
        "label": "18-30",
        "total_inc": df_age[(df_age["prtsn_age"] >= 18) & (df_age["prtsn_age"] <= 30)][
            "count"
        ].sum(),
        "type": "prtsn_age",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count partisans age >30<50
    data = {
        "label": "31-50",
        "total_inc": df_age[(df_age["prtsn_age"] > 30) & (df_age["prtsn_age"] <= 50)][
            "count"
        ].sum(),
        "type": "prtsn_age",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count partisans age >50
    data = {
        "label": ">50",
        "total_inc": df_age[df_age["prtsn_age"] > 50]["count"].sum(),
        "type": "prtsn_age",
    }
    df_total = pd.concat([df_total, pd.DataFrame(data, index=[0])], ignore_index=True)

    # Count partisans age
    df_age.columns = ["label", "total_inc"]
    df_age["label"] = "age_" + df_age["label"].astype(str)
    df_age["type"] = "prtsn_age"
    df_total = pd.concat([df_total, df_age], ignore_index=True)

    # reorder columns
    df_total = df_total[["type", "label", "total_inc"]]

    return df_total


@task(name="Incidents by year")
def incidents_by_year(df):
    """
    Incidents by year
    """

    cols_to_count = ["inc_type", "dmg_eqp"]

    df_tmp = pd.DataFrame()

    for year in df["year"].unique():
        df_year = df[df["year"] == year]

        for col in cols_to_count:
            df_count = count_incidents_by_column(df_year, col)
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

    df_tmp["label"] = df_tmp["label"].astype(str)
    df_tmp = df_tmp[["year", "type", "label", "total_inc"]]

    # pivot
    df_tmp = df_tmp.pivot_table(
        index=["type", "label"],
        columns="year",
        values="total_inc",
        fill_value=0,
    ).reset_index()

    return df_tmp


@task(name="Incidents by month")
def incidents_by_month(df):
    """
    incidents by month, year
    """

    df_tmp_1 = (
        df.groupby(["year", "month", "inc_type", "dmg_eqp"])
        .size()
        .reset_index(name="total_inc")
    )

    df_tmp_2 = df.groupby(["year", "month"]).size().reset_index(name="total_inc")

    df_tmp_3 = (
        df.groupby(["year", "month", "inc_type"]).size().reset_index(name="total_inc")
    )

    df_tmp_4 = (
        df.groupby(["year", "month", "dmg_eqp"]).size().reset_index(name="total_inc")
    )

    df_tmp = pd.concat([df_tmp_1, df_tmp_2, df_tmp_3, df_tmp_4], ignore_index=True)

    # add col month_year
    df_tmp["month_year"] = pd.to_datetime(
        df_tmp["year"].astype(str) + "-" + df_tmp["month"].astype(str)
    )

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
    print(df)

    df_fin = pd.DataFrame()

    for col in list_cols:
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
        df_tmp["cumul_inc"] = df_tmp.groupby([col])["cumul_inc"].fillna(method="ffill")

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
        df_tmp = (
            df.groupby(["region", col]).size().reset_index(name="total_inc")
        ).pivot_table(index="region", columns=col, values="total_inc", fill_value=0)

        df_final = pd.merge(df_final, df_tmp, on="region", how="outer")

    return df_final


@task(name="Incidents by incident type and damaged equipment")
def incident_type_damaged_equipment(df):
    """ """

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
        df.query("prtsn_grp.notnull()")
        .groupby(["dmg_eqp", "prtsn_grp"])
        .size()
        .reset_index(name="total_inc")
    )
    # ----------------------------------------------

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

    # Sankey Number of Apliicable Laws by Partisans Reward by Partisans Arrest
    source = []
    target = []
    value = []

    # df_tmp = df.query("partisans_arrest == True")

    df_tmp = (
        df.query("prtsn_arr == True")
        .groupby(["app_laws", "prtsn_rwd"])
        .size()
        .reset_index(name="total_inc")
    )

    # rename
    df_tmp["prtsn_rwd"] = df_tmp["prtsn_rwd"].replace(
        {True: "Reward", False: "No Reward"}
    )
    df_tmp["prtsn_arr"] = "Arrested"

    # Create a list of unique labels
    labels = list(
        set(["Arrested"] + list(df_tmp["prtsn_rwd"]) + list(df_tmp["app_laws"]))
    )

    # Create a mapping dictionary
    label_to_index = {label: i for i, label in enumerate(labels)}

    # Create source, target, and value lists
    source = (
        [
            label_to_index["Reward"]
            for _ in range(len(df_tmp.query("prtsn_rwd == 'Reward'")))
        ]
        + [
            label_to_index["No Reward"]
            for _ in range(len(df_tmp.query("prtsn_rwd == 'No Reward'")))
        ]
        + [label_to_index["Arrested"] for _ in range(len(df_tmp["prtsn_rwd"].unique()))]
    )

    # get index of app_laws where prtsn_rwd == 'Reward'
    target = (
        [
            label_to_index[laws]
            for laws in df_tmp.query("prtsn_rwd == 'Reward'")["app_laws"]
        ]
        + [
            label_to_index[laws]
            for laws in df_tmp.query("prtsn_rwd == 'No Reward'")["app_laws"]
        ]
        + [label_to_index["Reward"], label_to_index["No Reward"]]
    )

    # browse source and target
    for i in range(len(source) - 2):
        value.append(
            df_tmp.query(
                f"prtsn_rwd == '{labels[source[i]]}' and app_laws == '{labels[target[i]]}'"
            )["total_inc"].values[0]
        )

    value.append(
        df_tmp.query(f"prtsn_rwd == '{labels[target[-2]]}'")["total_inc"].sum()
    )
    value.append(
        df_tmp.query(f"prtsn_rwd == '{labels[target[-1]]}'")["total_inc"].sum()
    )

    # add missing values in labels
    labels = labels + [""] * (len(source) - len(labels))

    df_5 = pd.DataFrame(
        {
            "tab": "prtsn_rwd",
            "chart": "sankey",
            "label": labels,
            "source": source,
            "target": target,
            "value": value,
        }
    )

    df_final = pd.concat([df_1, df_2, df_3, df_4, df_5, df_6, df_7], ignore_index=True)

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

    # Get the unique ages
    ages = (
        df["prtsn_age"]
        .str.split(",")
        .explode()
        .str.strip()
        .replace("", np.nan)
        .dropna()
        .astype(int)
        .unique()
    )

    df = df.query("prtsn_age.notnull()")

    # Create a DataFrame to store the results
    df_final = pd.DataFrame()

    for age in ages:
        # Filter the DataFrame by the age
        df_age = df[df["prtsn_age"].str.contains(str(age))]

        # Count the number of applicable laws
        df_tmp = df_age["app_laws"].value_counts().reset_index()
        df_tmp.columns = ["app_laws", "total_inc"]
        df_tmp["age"] = age
        df_final = (
            pd.concat([df_final, df_tmp]).reset_index(drop=True).sort_values("age")
        )

    # remove rows "Law not indicated"
    df_final = df_final.query("app_laws != 'Law not indicated'")

    df_final = df_final.pivot_table(
        index="age",
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
    print(df_final)

    # decompose text (split by ,)
    df_final = (
        df_final["text"]
        .str.split(", ", expand=True)
        .stack()
        .reset_index()
        .drop(columns=["level_0", "level_1"])
    )
    df_final.columns = ["text"]
    print(df_final)

    # count words
    df_final = df_final["text"].value_counts().reset_index()
    df_final.columns = ["text", "total_inc"]
    print(df_final)

    return df_final


@flow(name="Create Datamarts Incidents Railway", log_prints=True)
def job_datamarts_incidents_railway():
    """
    Create Datamarts Incidents Railway
    """

    # read data from transform
    df = read_data(path_dw_transform, "incidents_railway")
    print(df)

    if df.empty:
        raise Failed("No data to process")

    # remove 2022
    # df = df[df["year"] != 2022]

    # Incidents total
    df_tmp = incidents_total(df)
    path = f"{path_dm_incidents_railway}/inc_total.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Incidents by year
    df_tmp = incidents_by_year(df)
    path = f"{path_dm_incidents_railway}/inc_by_year.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Incidents by month
    df_tmp = incidents_by_month(df)
    path = f"{path_dm_incidents_railway}/inc_by_month.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Incidents by day, week
    df_tmp = incidents_by_day_week(df)
    path = f"{path_dm_incidents_railway}/inc_by_day_week.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path)

    # Incidents by region
    df_tmp = incidents_by_region(df)
    path = f"{path_dm_incidents_railway}/inc_by_region.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path)

    # count occurrences of incident type and damaged equipment
    df_tmp = incident_type_damaged_equipment(df)
    path = f"{path_dm_incidents_railway}/inc_type_dmg_eqp.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Incidents by incident type and damaged equipment
    # For Graphs: Sunburst, Treemap
    df_tmp = incident_type_damaged_equipment_sunburst_treemap(df)
    path = f"{path_dm_incidents_railway}/inc_type_dmg_eqp_sun_tree.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Sabotage by partisans group
    df_tmp = sabotage_by_partisans_group(df)
    path = f"{path_dm_incidents_railway}/sabotage_by_prtsn_grp.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Applicable laws by partisans age
    df_tmp = applicable_laws_partisans_age(df)
    path = f"{path_dm_incidents_railway}/app_laws_prtsn_age.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path)

    # Wordcloud
    df_tmp = wordcloud(df)
    path = f"{path_dm_incidents_railway}/wordcloud.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)

    # Incidents cumul by month
    df_tmp = incidents_cumul_by_month(df)
    path = f"{path_dm_incidents_railway}/inc_cumul_by_month.parquet"
    # print(path)
    # print(df_tmp)
    df_tmp.to_parquet(path, index=False)
