from functools import lru_cache
from pathlib import Path

import duckdb
from fastapi import Response

EMPTY_JSON = "[]"

CACHE_HEADERS = {"Cache-Control": "public, max-age=300"}


@lru_cache(maxsize=256)
def _run(db_path: str, mtime_ns: int, query: str) -> str:
    """Execute a query on a DuckDB database and return the result as JSON.

    Args:
        db_path: Path to the DuckDB database file.
        mtime_ns: Modification time of the database file in nanoseconds.
        query: SQL query to execute.

    Returns:
        JSON string.
    """
    with duckdb.connect(db_path, read_only=True) as con:
        row = con.execute(query).fetchone()
    return row[0] if row and row[0] is not None else EMPTY_JSON


def execute_query(db_path: Path, query: str) -> str:
    """Execute a query on a DuckDB database and return the result as JSON.

    Args:
        db_path: Path to the DuckDB database file.
        query: SQL query to execute.

    Returns:
        JSON string.
    """
    try:
        mtime_ns = Path(db_path).stat().st_mtime_ns
    except FileNotFoundError:
        return EMPTY_JSON
    return _run(str(db_path), mtime_ns, query)


def make_endpoint(schema: str, table: str, db_path: Path):
    """Factory that creates a FastAPI endpoint returning JSON from a DuckDB table.

    Args:
        schema: DuckDB schema name (e.g. "incident_railway").
        table: Table name within the schema (e.g. "dmt_global").
        db_path: Path to the DuckDB database file.

    Returns:
        A callable suitable for ``router.add_api_route``.
    """
    query = f"""
        SELECT to_json(list(t))
        FROM (SELECT libelle, metric, value FROM {schema}.{table}) t
    """

    def endpoint() -> Response:
        return Response(
            content=execute_query(db_path, query),
            media_type="application/json",
            headers=CACHE_HEADERS,
        )

    return endpoint


def register_sections(router, sections: dict, schema: str, db_path: Path):
    """Register multiple section endpoints on a router.

    Args:
        router: FastAPI APIRouter instance.
        sections: Mapping of section name → table name (e.g. {"section1": "dmt_global"}).
        schema: DuckDB schema name.
        db_path: Path to the DuckDB database file.
    """
    for name, table in sections.items():
        router.add_api_route(
            f"/{name}",
            make_endpoint(schema, table, db_path),
            methods=["GET"],
            name=name,
            summary=f"Données de {table}",
        )
