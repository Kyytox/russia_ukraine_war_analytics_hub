from fastapi import APIRouter

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY as SCHEMA
from utils import register_sections

router = APIRouter(
    prefix="/api/incidents-railways/incident-types",
    tags=["incidents-railways-incident-types"],
)

SECTIONS = {
    "section1": "dmt_global",
    "section2": "dmt_incidents_types_by_month_cumul",
    "section3": "dmt_incidents_types_by_month",
    "section4": "dmt_incidents_types_by_year",
    "section5": "dmt_incidents_types_by_damaged_equipment",
    "section6": "dmt_incidents_types_by_region",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
