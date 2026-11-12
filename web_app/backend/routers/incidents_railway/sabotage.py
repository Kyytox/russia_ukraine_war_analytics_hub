from fastapi import APIRouter

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY as SCHEMA
from utils import register_sections

router = APIRouter(
    prefix="/api/incidents-railways/sabotage",
    tags=["incidents-railways-sabotage"],
)

SECTIONS = {
    "section1": "dmt_global",
    "section2": "dmt_sabotage_by_month",
    "section3": "dmt_incidents_total_vs_sabotage",
    "section4": "dmt_sabotage_by_prtsn_grp_over_time",
    "section5": "dmt_sabotage_by_prtsn_grp_by_damaged_equipment",
    "section6": "dmt_sabotage_by_damaged_equipment_by_prtsn_grp",
    "section7": "dmt_sabotage_by_damaged_equipment_by_month",
    "section8": "dmt_sabotage_by_region_by_damaged_equipment",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
