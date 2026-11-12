from fastapi import APIRouter

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY as SCHEMA
from utils import register_sections

router = APIRouter(
    prefix="/api/incidents-railways/collisions",
    tags=["incidents-railways-collisions"],
)

SECTIONS = {
    "section1": "dmt_global",
    "section2": "dmt_collisions_by_month",
    "section3": "dmt_collisions_by_damaged_equipment_by_month",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
