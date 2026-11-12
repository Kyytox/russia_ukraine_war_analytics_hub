from fastapi import APIRouter

router = APIRouter(prefix="/api/interactive-maps", tags=["interactive-maps"])

id_maps = [
    {
        "id": "map0",
        "image": "/img/img_map0.webp",
        "url": "https://ukrainewar.app/?events=true",
    },
    {
        "id": "map1",
        "image": "/img/img_map1.webp",
        "url": "https://deepstatemap.live/en#6/50.0571388/32.7612305",
    },
    {
        "id": "map2",
        "image": "/img/img_map2.webp",
        "url": "https://www.google.com/maps/d/u/0/viewer?mid=1xPxgT8LtUjuspSOGHJc2VzA5O5jWMTE&ll=47.50751714509709%2C34.14477250205766&z=6",
    },
    {
        "id": "map3",
        "image": "/img/img_map3.webp",
        "url": "https://www.warmapper.org/",
    },
    {
        "id": "map4",
        "image": "/img/img_map4.webp",
        "url": "https://eyesonrussia.org/",
    },
    {
        "id": "map5",
        "image": "/img/img_map5.webp",
        "url": "https://understandingwar.org/analysis/map-room/?_search_research=ATACMS&_teams=russia-ukraine",
    },
    {
        "id": "map6",
        "image": "/img/img_map6.webp",
        "url": "https://understandingwar.org/analysis/map-room/?_search_research=control&_teams=russia-ukraine",
    },
    {
        "id": "map7",
        "image": "/img/img_map7.webp",
        "url": "https://acleddata.com/monitor/ukraine-conflict-monitor",
    },
    {
        "id": "map8",
        "image": "/img/img_map8.webp",
        "url": "https://alerts.in.ua/en",
    },
    {
        "id": "map9",
        "image": "/img/img_map9.webp",
        "url": "https://map.ukrdailyupdate.com/?lat=49.385949&lng=32.744751&z=7&d=20384&c=1&l=0",
    },
    {
        "id": "map10",
        "image": "/img/img_map10.webp",
        "url": "https://www.google.com/maps/d/u/0/embed?mid=1lscRK6ehG0l2V-XvJ16nsyblMsQ&ll=48.32028801617995%2C38.452502603277026&z=8",
    },
    {
        "id": "map11",
        "image": "/img/img_map11.webp",
        "url": "https://ukraine.bellingcat.com/",
    },
    {
        "id": "map12",
        "image": "/img/img_map12.webp",
        "url": "https://militarysummary.com/map",
    },
    {
        "id": "map13",
        "image": "/img/img_map13.webp",
        "url": "https://theundeniablestreetview.com/",
    },
]


@router.get("/list-maps")
def list_maps():
    """
    List all interactive maps with their details.
    """
    return id_maps
