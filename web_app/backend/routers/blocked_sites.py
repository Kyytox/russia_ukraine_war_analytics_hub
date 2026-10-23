import duckdb
from fastapi import APIRouter
from config import DB_PATH, SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES

from fastapi import Response

router = APIRouter(prefix="/api/blocked-sites", tags=["blocked-sites"])


@router.get("/kpi")
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
            FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
            WHERE libelle = 'total'

            UNION ALL

            SELECT 'kpi2' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
                WHERE libelle = 'by_country_domain'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi3' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
                WHERE libelle = 'by_category'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t

            UNION ALL

            SELECT 'kpi4' AS libelle, metric as value FROM (
                SELECT metric as metric, MAX(count) AS max_count
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
                WHERE libelle = 'by_banning_authority'
                GROUP BY metric
                ORDER BY max_count DESC
                LIMIT 1
            ) t
                
            UNION ALL

            SELECT 'last_date_updated' AS libelle, metric as value FROM (
                SELECT metric as metric
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
                WHERE libelle = 'last_date_updated'
                LIMIT 1
            ) t            ; 
            """
        ).fetchall()

        return [{"id": libelle, "value": value} for libelle, value in rows]

    return []


@router.get("/section1")
def section1():
    """
    Data for Charts of Section 1
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
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_global
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section2")
def section2():
    """
    Data for Charts of Section 2
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle as libelle,
                    date as metric,
                    count
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_by_date
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section3")
def section3():
    """
    Data for Charts of Section 3
    """

    if not DB_PATH.exists():
        return []

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    id,
                    month as libelle,
                    metric,
                    count
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_metrics_over_time
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section4")
def section4():
    """
    Data for Charts of Section 4
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
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_by_authority_category
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section5")
def section5():
    """
    Data for Charts of Section 5
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
                    value
                FROM {SCHEMA_DUCKDB_DW_DMT_BLOCKED_SITES}.dmt_by_country_by_category_top_6
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []
