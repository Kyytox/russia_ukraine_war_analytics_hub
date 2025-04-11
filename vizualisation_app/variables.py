# Data paths
path_dmt_inc_railway = "data/data_warehouse/datamarts/incidents_railway"
path_dmt_block_site = "data/data_warehouse/datamarts/russia_block_sites"
PATH_DMT_COMPO_WEAPONS = "data/data_warehouse/datamarts/compo_weapons"


# Home page content
DICT_CONTENT = {
    "Incidents Russian Railways": {
        "title": "Incidents Russian Railways",
        "text": "Explore incidents related to Russian Railways since 2022.",
        "image": "assets/images/inc_rail.jpeg",
        "url": "/incidents-railway",
        "tags": ["Analytics", "Graphs", "Data Sources"],
        "color_tags": ["#961010", "#4abb15", "#14aca4"],
        "icon": "fa-train",
    },
    "Blocked Web Sites in Russia": {
        "title": "Blocked Web Sites in Russia",
        "text": "Explore the list of websites blocked in Russia.",
        "image": "assets/images/logo_blocked_sites.png",
        "url": "/blocked-sites",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
        "icon": "fa-ban",
    },
    "Raid Alerts in Ukraine": {
        "title": "Raid Alerts in Ukraine",
        "text": "Explore the alerts related to missile raids in Ukraine.",
        "image": "assets/images/raid_alerts_ukraine.png",
        "url": "/raids-alerts",
        "tags": ["Analytics", "Graphs", "Data Sources", "External Data"],
        "color_tags": ["#961010", "#4abb15", "#14aca4", "#821f9b"],
        "icon": "fa-bell",
    },
    "Components Weapons": {
        "title": "Components Weapons",
        "text": "Explore the components used in weapons.",
        "image": "assets/images/components_weapons.jpg",
        "width": 300,
        "url": "/components-waepons",
        "tags": ["Analytics", "Graphs", "Data Sources"],
        "color_tags": ["#961010", "#4abb15", "#14aca4"],
        "icon": "fa-cog",
    },
}

# Colors Globals
PAPER_BGCOLOR = "#302c2c"
PLOT_BGCOLOR = "#302c2c"

# Data colors
colors = {
    #
    2022: "#355070",
    "2022": "#355070",
    2023: "#6D597A",
    "2023": "#6D597A",
    2024: "#B56576",
    "2024": "#B56576",
    2025: "#d85f63",
    "2025": "#d85f63",
    #
    "Total": "rgb(102, 80, 80)",
    "Sabotage": "#6a040f",
    "Fire": "#c71f37",
    "Derailment": "#ff6700",
    "Collision": "#fab77c",
    "Other": "#f79618",
    "Attack": "#ffba08",
    #
    "Freight Train": "#001A6E",
    "Passengers Train": "#074799",
    "Locomotive": "#608BC1",
    "Relay Cabin": "#0A5EB0",
    "Infrastructure": "#F0FFFF",
    "Railroad Tracks": "#5439c0",
    "Electric Box": "#846abb",
    #
    "Human": "#ffc8dd",
    "Car": "#eb99b7",
    "Truck": "#c77dff",
    "Train": "#7b2cbf",
    "Object": "#3c096c",
    #
    "Rospartizan Group": "#2B9348",
    "Freedom Russia Legion": "#035e47",
    "No affiliation": "#007F5F",
    "Green Gendarmerie": "#52b788",
    "ATESH": "#99d98c",
    "Russian Volunteer Corps": "#55A630",
    "Right of Power": "#80B918",
    "GUR": "#BFD200",
    "Wanted": "#AACC00",
    "BOAK": "#a2a50e",
    "Stop the Wagons": "#FFFF3F",
    #
    "Total Arrested": "rgb(0, 128, 128)",
    "Sabotage with Arrest": "rgb(13, 141, 9)",
    "Sabotage without Arrest": "rgb(39, 77, 39)",
    "Reward": "rgb(22, 173, 184)",
    "No Reward": "rgb(46, 113, 117)",
    #
    "Unknown": "#D3D3D3",
    "167 - Intentional Destruction or Damage of Property": "#bb4b4d",
    "281 - Sabotage": "#f8961e",
    "267 - Damage Transport Vehicles or Communications": "#f9c74f",
    "205-205.5 - Terrorism": "#7ea75f",
    "205 - Terrorism": "#7ea75f",
    "213 - Hooliganism": "#4d908e",
    "Go To WAR": "#e60909",
    #
    "<18": "#1c7897",
    "18-30": "#63a063",
    "31-50": "#FFFF00",
    ">50": "#FFA500",
}


COLORS_COMP_WEAPONS = {
    "UAV": "#581e88",
    "Equipment": "#75248d",
    "Missile": "#9e1f94",
    "Vehicule": "#881d53",
    #
    #
    "UAV Combat": "#b8747a",
    "UAV Surveillance": "#A8DADC",
    "Electronic Equipment": "#4976b6",
    "Communication Equipment": "#446275",
    "Cruise Missile": "#F4A261",
    "Ballistic Missile": "#E76F51",
    "UAV Combat and Surveillance": "#F9C74F",
    "Armored Vehicle": "#4ab695",
    "Navigation Equipment": "#4D908E",
    "Helicopter": "#1c2e3d",
    "Guided Missile": "#b81f22",
    "Artillery": "#90BE6D",
    "Glide Bomb": "#F8961E",
    "FPV": "#9C6644",
    "Air Defense": "#6A4C93",
}
