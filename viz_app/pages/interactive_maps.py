import dash
from dash import html

from assets.components.cards import create_card


dash.register_page(__name__)


page_content = {
    "card1": {
        "title": "DeepState Map of Ukraine",
        "text": "News of russia's war against Ukraine on the map.",
        "image": "./assets/images/img_map_card1.png",
        "url": "https://deepstatemap.live/en#6/50.0571388/32.7612305",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card2": {
        "title": "UA Control Map",
        "text": "UA Control And Units Map.",
        "image": "./assets/images/img_map_card2.png",
        "url": "https://www.google.com/maps/d/u/0/viewer?mid=1xPxgT8LtUjuspSOGHJc2VzA5O5jWMTE&ll=47.50751714509709%2C34.14477250205766&z=6",
        "tags": ["Map", "Units"],
        "color_tags": ["#121096", "#aaa824"],
    },
    "card3": {
        "title": "War Mapper",
        "text": "Understand the impact of the frontline changes in Ukraine through a series of control charts.",
        "image": "./assets/images/img_map_card3.png",
        "url": "https://www.warmapper.org/",
        "tags": ["Map", "Graphs"],
        "color_tags": ["#121096", "#4abb15"],
    },
    "card4": {
        "title": "Eyes on Russia",
        "text": "Map draws on the database of videos, photos, satellite imagery or other media related to Russia’s invasion of Ukraine.",
        "image": "./assets/images/img_map_card4.png",
        "url": "https://eyesonrussia.org/",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card5": {
        "title": "ISW: Russian Military Objects Are in Range of ATACMS",
        "text": "Map allows users to inspect 225 known Russian military objects in Russia that are in range of Ukrainian ATACMS.",
        "image": "./assets/images/img_map_card5.png",
        "url": "https://understandingwar.org/analysis/map-room/?_search_research=ATACMS&_teams=russia-ukraine",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card6": {
        "title": "ISW: Interactive Map: Russia's Invasion of Ukraine",
        "text": "Interactive maps complements the static control-of-terrain maps that ISW daily produces with high-fidelity and, where possible, street level assessments of the war in Ukraine.",
        "image": "./assets/images/img_map_card6.png",
        "url": "https://understandingwar.org/analysis/map-room/?_search_research=control&_teams=russia-ukraine",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card7": {
        "title": "Ukraine Conflict Monitor",
        "text": "ACLED’s Ukraine Conflict Monitor provides near real-time information on the ongoing war, including an interactive map, data file, weekly situation updates. Designed to help researchers, policymakers, media, and the wider public track key conflict developments in Ukraine.",
        "image": "./assets/images/img_map_card7.png",
        "url": "https://acleddata.com/monitor/ukraine-conflict-monitor",
        "tags": ["Map", "Data Sources"],
        "color_tags": ["#121096", "#14aca4"],
    },
    "card8": {
        "title": "Air Raid Alert Map of Ukraine",
        "text": "Showcases air raid alerts across Ukraine on an interactive map, providing real-time updates on siren activations and safety information.",
        "image": "./assets/images/img_map_card8.png",
        "url": "https://alerts.in.ua/en",
        "tags": ["Map", "Analytics"],
        "color_tags": ["#121096", "#961010"],
    },
    "card9": {
        "title": "Ukraine Daily Updates",
        "text": "Map maintained by Andrew Perpetua, providing the daily events of this war and the current situation on the battlefield. Notable events affecting military targets and civilian victims, fortifications and much more is shown through the map’s adjustable layers.",
        "image": "./assets/images/img_map_card9.png",
        "url": "https://map.ukrdailyupdate.com/?lat=49.385949&lng=32.744751&z=7&d=20384&c=1&l=0",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card10": {
        "title": "Timeline of Invasion",
        "text": "Map by David Batashvili, visualize the movements of armies during Russia's invasion of Ukraine from February 24, 2022.",
        "image": "./assets/images/img_map_card10.png",
        "url": "https://www.google.com/maps/d/u/0/embed?mid=1lscRK6ehG0l2V-XvJ16nsyblMsQ&ll=48.32028801617995%2C38.452502603277026&z=8",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card11": {
        "title": "Civilian Harm Map",
        "text": "Incidents in Ukraine that have resulted in potential civilian harm. These include: incidents where rockets or missiles struck civilian areas, where attacks have resulted in the destruction of civilian infrastructure, where the presence of civilian injuries are visible and/or the presence of immobile civilian bodies.",
        "image": "./assets/images/img_map_card11.png",
        "url": "https://ukraine.bellingcat.com/",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
    "card12": {
        "title": "Fortifications in Ukraine",
        "text": "Map showing fortifications in Ukraine including trenches, bunkers, and other defensive structures built during the conflict.",
        "image": "./assets/images/img_map_card12.png",
        "url": "https://militarysummary.com/map",
        "tags": ["Map"],
        "color_tags": ["#121096"],
    },
}


layout = html.Div(
    className="page-content",
    children=[
        # Header
        html.H1(
            className="page-title",
            children="Interactive Maps 🗺️",
        ),
        # Cards
        html.Div(
            [
                create_card(
                    data["title"],
                    data["text"],
                    data["image"],
                    data["url"],
                    data["tags"],
                    data["color_tags"],
                )
                for key, data in page_content.items()
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "flexWrap": "wrap",
                "justifyContent": "center",
            },
        ),
    ],
)
