import duckdb
from fastapi import APIRouter
from config import (
    DB_PATH,
    SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY,
)

from fastapi import Response

router = APIRouter(
    prefix="/api/incidents-railways/incident-types",
    tags=["incidents-railways-incident-types"],
)

schema = SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY


@router.get("/section1")
def section1():
    """Return section 1 data from dmt_global table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    value,
                FROM {schema}.dmt_global
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section2")
def section2():
    """Return section 2 data from dmt_incidents_by_month table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    value
                FROM {schema}.dmt_incidents_types_by_month_cumul
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section3")
def section3():
    """Return section 3 data from dmt_incidents_types_by_month table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    value
                FROM {schema}.dmt_incidents_types_by_month
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section4")
def section4():
    """Return section 4 data from dmt_incidents_by_region table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    value
                FROM {schema}.dmt_incidents_types_by_year
            ) t
            """
        ).fetchone()[0]

        # WHERE label <> 'Moscow'
        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section5")
def section5():
    """Return section 5 data from dmt_incidents_types_by_damaged_equipment table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    parent,
                    label,
                    value
                FROM {schema}.dmt_incidents_types_by_damaged_equipment
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section6")
def section6():
    """Return section 6 data from dmt_incidents_types_by_region table."""
    if not DB_PATH.exists():
        return Response(content="[]", media_type="application/json")

    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            f"""
            SELECT to_json(list(t))
            FROM (
                SELECT
                    libelle,
                    metric,
                    value
                FROM {schema}.dmt_incidents_types_by_region
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []
