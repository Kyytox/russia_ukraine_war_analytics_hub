import duckdb
from fastapi import APIRouter

from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS as SCHEMA
from utils import register_sections

router = APIRouter(prefix="/api/components-weapons", tags=["components-weapons"])


@router.get("/summary")
def summary():
    """
    KPI summary. Kept as-is (custom UNION ALL query).
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT 'kpi1' AS libelle, SUM(value) AS value
            FROM {SCHEMA}.dmt_global_count
            WHERE libelle = 'type_equipment'

            UNION ALL

            SELECT 'kpi2' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(value) AS max_count
                FROM {SCHEMA}.dmt_global_count
                WHERE libelle = 'manufacturer_country'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi3' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(value) AS max_count
                FROM {SCHEMA}.dmt_global_count
                WHERE libelle = 'manufacturer'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi4' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(value) AS max_count
                FROM {SCHEMA}.dmt_global_count
                WHERE libelle = 'weapon'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t;
            """
        ).fetchall()

        return [{"id": libelle, "value": value} for libelle, value in rows]

    return []


SECTIONS = {
    "section1": "dmt_global_count",
    "section2": "dmt_country_by_weapon",
    "section3": "dmt_treemap_country_weapon",
    "section4": "dmt_sunburst_equipments_weapon_type",
}

register_sections(router, SECTIONS, SCHEMA, DB_PATH)
