import duckdb
from fastapi import APIRouter
from config import (
    DB_PATH,
    SCHEMA_DUCKDB_DW_DMT_INCIDENT_RAILWAY,
)

from fastapi import Response

router = APIRouter(
    prefix="/api/incidents-railways/partisans-arrest",
    tags=["incidents-railways-partisans-arrest"],
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
    """Return section 2 data from dmt_partisans_arrested_by_age table."""
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
                FROM {schema}.dmt_partisans_arrested_by_age
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section3")
def section3():
    """Return section 3 data from dmt_partisans_arrested_by_group_age table."""
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
                FROM {schema}.dmt_partisans_arrested_by_group_age
            ) t
            """
        ).fetchone()[0]

        return Response(content=rows or "[]", media_type="application/json")

    return []


@router.get("/section4")
def section4():
    """Return section 4 data from dmt_partisans_arrested_by_applicable_laws table."""
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
                FROM {schema}.dmt_partisans_arrested_by_applicable_laws
            ) t
            """
        ).fetchone()[0]

        # WHERE metric <> 'Moscow'
        return Response(content=rows or "[]", media_type="application/json")

    return []
