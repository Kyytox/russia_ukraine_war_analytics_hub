# config.py
from pathlib import Path

DB_PATH = (
    Path(__file__).parent.parent.parent / "data/data_warehouse/gold/dwh_datamarts.db"
)


SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY = "incident_railway"
SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS = "components_weapons"
SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES = "blocked_sites"
