import os
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from prefect import task

from core.libs.utils import generate_task_name


@task(
    name="Read data from parquet",
    task_run_name=generate_task_name,
    tags=["read"],
)
def read_parquet(base_path: str, file_name: str) -> pd.DataFrame:
    """
    Read data from parquet dataset using pyarrow.dataset

    Args:
        base_path: base path
        file_name: file name (acts as directory or parquet dataset path)

    Returns:
        Dataframe with parquet data
    """
    path = os.path.join(base_path, f"{file_name}.parquet")
    print(f"Reading data from {path}")

    try:
        if os.path.exists(path):
            df = ds.dataset(path, format="parquet").to_table().to_pandas()
        else:
            df = pd.DataFrame()
    except Exception as e:
        print(e)
        df = pd.DataFrame()

    print(f"Reading {df.shape} data from {path}")
    return df


@task(name="Save data from parquet", task_run_name=generate_task_name, tags=["save"])
def save_parquet(
    base_path: str, file_name: str, df: pd.DataFrame, partition_cols: list | None = None
):
    """
    Save data to parquet using pyarrow.dataset with proper overwrite behavior

    Args:
        base_path: base path
        file_name: file name
        df: dataframe to save
        partition_cols: list of columns to partition

    Returns:
        None
    """

    if df.empty:
        print("No data to save")
        return

    # create folder if not exist
    if not os.path.exists(base_path):
        os.makedirs(base_path)

    path = os.path.join(base_path, f"{file_name}.parquet")

    # Convert DataFrame to pyarrow Table
    table = pa.Table.from_pandas(df)

    # Save dataset
    # existing_data_behavior="overwrite_or_ignore" permet d'écraser proprement les partitions existantes
    ds.write_dataset(
        table,
        base_dir=path,
        format="parquet",
        # partitioning=partition_cols if partition_cols else None,
        existing_data_behavior="overwrite_or_ignore",
    )

    print(f"Saving {df.shape} data to {path}")
    return
