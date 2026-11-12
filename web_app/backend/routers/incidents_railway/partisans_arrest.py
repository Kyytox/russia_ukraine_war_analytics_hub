from fastapi import APIRouter

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY as SCHEMA
from utils import register_sections

router = APIRouter(
    prefix="/api/incidents-railways/partisans-arrest",
    tags=["incidents-railways-partisans-arrest"],
)

SECTIONS = {
    "section1": "dmt_global",
    "section2": "dmt_partisans_arrested_by_age",
    "section3": "dmt_partisans_arrested_by_group_age",
    "section4": "dmt_partisans_arrested_by_applicable_laws",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
