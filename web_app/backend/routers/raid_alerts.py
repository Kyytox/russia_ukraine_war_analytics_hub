from fastapi import APIRouter

router = APIRouter(prefix="/api/raid-alerts", tags=["raid-alerts"])


@router.get("/summary")
def summary():
    """KPI de la page (à implémenter avec DuckDB)."""
    return {}


@router.get("/timeseries")
def timeseries():
    """Données des graphiques (à implémenter)."""
    return []
