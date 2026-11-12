import duckdb
from fastapi import APIRouter, Response

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY as SCHEMA
from utils import register_sections

router = APIRouter(
    prefix="/api/incidents-railways/overview", tags=["incidents-railways-overview"]
)


@router.get("/kpi")
def summary():
    """Return KPI summary from dmt_kpis table. Kept as-is (custom query)."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle AS id,
                    value
                FROM {SCHEMA}.dmt_kpis
                WHERE metric IN (
                    'last_date_updated',
                    'total_incidents',
                    'total_sabotage',
                    'total_arrested_partisans'
                )
            ) t
            """
        ).fetchall()

        return Response(content=rows[0][0], media_type="application/json")

    return []


SECTIONS = {
    "section1": "dmt_global",
    "section2": "dmt_incidents_by_month",
    "section3": "dmt_incidents_by_day",
    "section4": "dmt_incidents_by_region",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
