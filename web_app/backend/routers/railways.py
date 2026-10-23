from fastapi import APIRouter

router = APIRouter(prefix="/api/railways", tags=["railways"])


@router.get("/summary")
def summary():
    """KPI de la page (à implémenter avec DuckDB)."""
    return {}


@router.get("/timeseries")
def timeseries():
    """Données des graphiques (à implémenter)."""
    return []
