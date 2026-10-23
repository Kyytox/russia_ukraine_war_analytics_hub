from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import DB_PATH

from routers import (
    blocked_sites,
    losses,
    maps,
    raid_alerts,
    railways,
    components_weapons,
)

app = FastAPI(title="Russia-Ukraine War Data Hub")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

for r in (railways, blocked_sites, raid_alerts, components_weapons, maps, losses):
    app.include_router(r.router)

# Sections de la page d'accueil. Pas de texte ici : titres et descriptions
# sont traduits côté front (frontend/src/i18n/locales/*.json).
DICT_CONTENT = [
    {
        "id": "incidents-railways",
        "image": "/img/inc_rail.jpeg",
        "url": "/incidents-railway",
        "tags": ["Analytics", "Graphs", "Data Sources"],
        "color_tags": ["#961010", "#4abb15", "#14aca4"],
        "icon": "fa-train",
    },
    {
        "id": "blocked-sites",
        "title": "Blocked Web Sites in Russia",
        "descr": "Explore the list of websites blocked in Russia.",
        "image": "/img/logo_blocked_sites.png",
        "url": "/blocked-sites",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
        "icon": "fa-ban",
    },
    {
        "id": "raid-alerts",
        "title": "Raid Alerts in Ukraine",
        "descr": "Explore the alerts related to missile raids in Ukraine.",
        "image": "/img/raid_alerts_ukraine.png",
        "url": "/raids-alerts",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
        "icon": "fa-bell",
    },
    {
        "id": "components-weapons",
        "title": "Components in Aggressor's Weapons",
        "descr": "Explore the components used in the aggressor`s weapon.",
        "image": "/img/components_weapons.jpg",
        "url": "/components-waepons",
        "tags": ["Analytics", "Graphs", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#821f9b"],
        "icon": "fa-cog",
    },
    {
        "id": "interactive-maps",
        "title": "Interactive Maps",
        "descr": "Explore the interactive maps related to the conflict.",
        "image": "/img/interactive_maps.png",
        "url": "/interactive-maps",
        "tags": ["Maps", "External Data"],
        "color_tags": ["#121096", "#821f9b"],
        "icon": "fa-map",
    },
    {
        "id": "military-losses",
        "title": "Military Losses",
        "descr": "Explore the military losses in Ukraine.",
        "image": "/img/military_losses.png",
        "url": "/military-losses",
        "tags": ["Maps", "Graphs", "Data Sources"],
        "color_tags": ["#121096", "#4abb15", "#14aca4"],
        "icon": "fa-cross",
    },
]


@app.get("/api/sections")
def sections():
    return DICT_CONTENT


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
