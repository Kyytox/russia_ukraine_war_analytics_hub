import os
import pandas as pd
import numpy as np

from prefect import flow, task
from prefect.states import Completed, Failed


# Variables
from core.config.paths import (
    PATH_DWH_SOURCES,
    PATH_DMT_INC_RAILWAY,
)

# Utils
from core.libs.utils import read_data, save_data


@flow(
    name="DWH Subflow Datamarts Components in Weapons",
    flow_run_name="dwh-subflow-dmt-compo-weapons",
    log_prints=True,
)
def flow_dmt_compo_weapons():
    """
    Datamarts Components in Weapons
    """
    return Completed(message="All DataMarts Components in Weapons Completed")
