import duckdb
from fastapi import APIRouter
from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS

from fastapi import Response

router = APIRouter(prefix="/api/components-weapons", tags=["components-weapons"])


@router.get("/summary")
def summary():
    """
    KPI summary
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT 'kpi1' AS libelle, SUM(count) AS value
            FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_global_count
            WHERE libelle = 'type_equipment'

            UNION ALL

            SELECT 'kpi2' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_global_count
                WHERE libelle = 'manufacturer_country'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi3' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_global_count
                WHERE libelle = 'manufacturer'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi4' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_global_count
                WHERE libelle = 'weapon'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t; 
            """
        ).fetchall()

        return [{"id": libelle, "value": value} for libelle, value in rows]

    return []


@router.get("/section1")
def section1():
    """
    Get data for 4 graphs
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    count
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_global_count
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section2")
def section2():
    """
    Get data for 4 graphs
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
               SELECT 
                    libelle,
                    metric, 
                    count
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_country_by_weapon
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section3")
def section3():
    """
    Get data for 4 graphs
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
               SELECT 
                    parent, 
                    label,
                    value, 
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_treemap_country_weapon
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section4")
def section4():
    """
    Get data for 4 graphs
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
               SELECT 
                    parent, 
                    label,
                    value, 
                FROM {SCHEMA_DUCKDB_DW_DMT_COMPONENTS_WEAPONS}.dmt_sunburst_equipments_weapon_type
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []
