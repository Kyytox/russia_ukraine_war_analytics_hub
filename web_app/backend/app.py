from datetime import date
from pathlib import Path

import duckdb
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from config import DB_PATH

from routers import (
    blocked_sites,
    military_losses,
    interactive_maps,
    raid_alerts,
    components_weapons,
)

from routers.incidents_railway import (
    collisions,
    damaged_equipments,
    overview,
    incident_types,
    sabotage,
    partisans_arrest,
    informations,
)

app = FastAPI(title="Russia-Ukraine War Data Hub")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

for r in (
    overview,
    incident_types,
    damaged_equipments,
    collisions,
    sabotage,
    partisans_arrest,
    informations,
    blocked_sites,
    raid_alerts,
    components_weapons,
    interactive_maps,
    military_losses,
):
    app.include_router(r.router)

# Sections de la page d'accueil. Pas de texte ici : titres et descriptions
# sont traduits côté front (frontend/src/i18n/locales/*.json).
DICT_CONTENT = [
    {"id": "incidents-railways", "image": "/img/inc_rail.jpeg"},
    {"id": "blocked-sites", "image": "/img/logo_blocked_sites.png"},
    {"id": "raid-alerts", "image": "/img/raid_alerts_ukraine.png"},
    {"id": "components-weapons", "image": "/img/components_weapons.jpg"},
    {"id": "interactive-maps", "image": "/img/interactive_maps.png"},
    {"id": "military-losses", "image": "/img/military_losses.png"},
]


@app.get("/api/sections")
def sections():
    return DICT_CONTENT


@app.get("/api/day-war")
def day_war():
    """Get the number of days since the war started (24 February 2022)."""

    start_date = date(2022, 2, 24)
    today = date.today()
    delta = today - start_date
    return delta.days


@app.get("/api/losses/monthly")
def losses_monthly():
    """Exemple DuckDB : agrégation mensuelle depuis un fichier parquet/csv.
    Adapte la table `losses(date, category, count)` à tes données."""
    if not DB_PATH.exists():
        return []  # pas encore de base : le front gère le vide
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        rows = con.execute(
            """
            SELECT strftime(date_trunc('month', date), '%Y-%m') AS month,
                   category, SUM(count) AS total
            FROM losses GROUP BY 1, 2 ORDER BY 1
            """
        ).fetchall()
    return [{"month": m, "category": c, "total": t} for m, c, t in rows]
