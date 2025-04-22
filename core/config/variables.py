#############
## GLOBALS ##
#############


# Path Excel file Incident Railway
ID_EXCEL_INCIDENT_RAILWAY = "1jyD1bB0uauqIo-Bsi_qoBqV9JAu7cUXvG0UzZmFrSPk"
ID_EXCEL_RUSSIA_BLOCK_SITE = "1KN3isOEE7A4vBL9-QbEd-gQUgYcg4Hn50pKHJ75SS-A"
ID_EXCEL_INCIDENT_ARREST = "1yMzTidLDj-sdsACYMkhRFuGIGOtscvpvijAAmVlLCfM"
# ID_EXCEL_INCIDENT_SABOTAGE = "1jyD1bB0uauqIo-Bsi_qoBqV9JAu7cUXvG0UzZmFrSPk"

# IA OLLAMA
# IA_TRANSLATE = "ia_translate:latest" # hermes 3
IA_TRANSLATE = "ia_translate_2:latest"
# IA_CLASSIFY = "ia_classify:latest"
IA_CLASSIFY = "mistral-nemo:latest"

GROUP_SIZE_TO_TRANSLATE = 40
SIZE_TO_TRANSLATE = 19
SIZE_TO_QUALIF = 199

###############
## PROMPT IA ##
###############
PROMPT_RAILWAY = "Does the message contain information about a railway incident ?"
PROMPT_ARREST = "Does the message contain information about an arrest or sentence ?"
PROMPT_SABOATGE = "Does the message contain information about sabotage, arson, vandalism or acts against Russian infrastructure or government ?"

#
LIST_ACCOUNTS_TELEGRAM = [
    "electrichki",
    "astrapress",
    "shot_shot",
    "belzhd_live",
    "atesh_ua",
    "legionoffreedom",
    "VDlegionoffreedom",
    "ENews112",
    "telerzd",
    "mzd_rzd",
    "magistral_kuvalda",
    "nskzd",
    "news_zszd",
    "D4msk",
    "rospartizan",
    "boakom",
    "rdpsru",
    "ostanovi_vagony_2023",
    "activatica",
    "russvolcorps",
    "soprotivleniye_lsr",
    "Sib_EXpress",
    "algizrpd",  # t.me/algizrpd
    "idelrealii",  # t.me/idelrealii
    "mchsYakutia",
    "ostorozhno_novosti",
]


DICT_UTC = {
    "astrapress": 2,
    "shot_shot": 3,
    "electrichki": 3,
    "belzhd_live": 3,
    "atesh_ua": 2,
    "legionoffreedom": 3,
    "VDlegionoffreedom": 3,
    "ENews112": 3,
    "telerzd": 3,
    "mzd_rzd": 3,
    "magistral_kuvalda": 3,
    "nskzd": 7,
    "news_zszd": 8,
    "D4msk": 3,
    "rospartizan": 3,
    "boakom": 3,
    "rdpsru": 3,
    "ostanovi_vagony_2023": 3,
    "activatica": 3,
    "russvolcorps": 3,
    "soprotivleniye_lsr": 3,
    "Sib_EXpress": 7,
    "algizrpd": 3,
    "idelrealii": 3,
    "mchsYakutia": 7,
    "ostorozhno_novosti": 3,
}


LIST_REGIONS = [
    "Adygey",
    "Altay",
    "Amur",
    "Arkhangelsk",
    "Astrakhan",
    "Bashkortostan",
    "Belgorod",
    "Bryansk",
    "Buryat",
    "Chechnya",
    "Chelyabinsk",
    "Chita",
    "Chukchi Autonomous Okrug",
    "Chuvash",
    "Crimea",
    "Dagestan",
    "Gorno-Altay",
    "Ingush",
    "Irkutsk",
    "Ivanovo",
    "Kabardin-Balkar",
    "Kaliningrad",
    "Kalmyk",
    "Kaluga",
    "Kamchatka",
    "Karachay-Cherkess",
    "Karelia",
    "Kemerovo",
    "Khabarovsk",
    "Khakass",
    "Khanty-Mansiy",
    "Kirov",
    "Komi",
    "Kostroma",
    "Krasnodar",
    "Krasnoyarsk",
    "Kurgan",
    "Kursk",
    "Leningrad",
    "Lipetsk",
    "Maga Buryatdan",
    "Mariy-El",
    "Mordovia",
    # "Moscow City",
    # "Moscow Oblast",
    "Moscow",
    "Murmansk",
    "Nenets",
    "Nizhegorod",
    "North Ossetia",
    "Novgorod",
    "Novosibirsk",
    "Omsk",
    "Orel",
    "Orenburg",
    "Penza",
    "Perm",
    "Primorye",
    "Pskov",
    "Rostov",
    "Ryazan",
    "Sakha (Yakutia)",
    "Sakhalin",
    "Samara",
    "Saratov",
    "Smolensk",
    "St.Petersburg City",
    "Stavropol",
    "Sverdlovsk",
    "Tambov",
    "Tatarstan",
    "Tomsk",
    "Transbaikalia",
    "Tula",
    "Tuva",
    "Tver",
    "Tyumen",
    "Udmurt",
    "Ulyanovsk",
    "Vladimir",
    "Volgograd",
    "Vologda",
    "Voronezh",
    "Yamal-Nenets",
    "Yaroslavl",
    "Yevrey",
]


LIST_LAWS = [
    "167 - Intentional Destruction or Damage of Property",
    "281 - Sabotage",
    "205 - Terrorism",
    "275 - Treason",
    "205.2 - Public Promotion or Justification of Terrorism",
    "207 - Knowingly Making a False Threat of Terrorism",
    "208 - Organisation or Participation of an Illegal Armed Formation",
    "Unknown",
]


DICT_LAWS = {
    "167 - Intentional Destruction or Damage of Property": [
        "article 167",
        "167.",
        "167 of the Criminal",
    ],
    "281 - Sabotage": ["article 281", "281.", "281 of the Criminal"],
    "205 - Terrorism": ["article 205", "205.1", "205.", "205 of the Criminal"],
    "275 - Treason": ["article 275", "275.", "275 of the Criminal"],
    "205.2 - Public Promotion or Justification of Terrorism": [
        "article 205.2",
        "205.2",
        "205.2 of the Criminal",
    ],
    "207 - Knowingly Making a False Threat of Terrorism": [
        "article 207",
        "207.",
        "207 of the Criminal",
    ],
    "208 - Organisation or Participation of an Illegal Armed Formation": [
        "article 208",
        "208.",
        "208 of the Criminal",
    ],
}

LIST_INC_TYPE_RAIL = [
    "Derailment",
    "Sabotage",
    "Fire",
    "Collision",
    "Attack",
    "Other",
]

LIST_DMG_EQUIP_RAIL = [
    "Freight Train",
    "Passengers Train",
    "Locomotive",
    "Relay Cabin",
    "Infrastructure",
    "Railroad Tracks",
    "Electric Box",
    "Unknown",
]

LIST_COLL_WITH_RAIL = [
    "Human",
    "Train",
    "Car",
    "Truck",
    "Object",
]


LIST_PARTISANS_GRP = [
    "Wanted",
    "No affiliation",
    "ATESH",
    "Freedom Russia Legion",
    "GUR",
    "Rospartizan Group",
    "Russian Volunteer Corps",
    "Right of Power",
    "Ukrainian Army",
    "RevAnarchoFond",
    "BOAK",
    "Green Gendarmerie",
    "Stop the Wagons",
]


LIST_ARREST_REASON = [
    "Post on social media",
    "Arson",
    "Attempt Sabotage",
    "Sabotage",
    "Kill",
    "Protest",
    "Anti-war Protest",
]

LIST_JOBS = [
    "Military",
    "Politician",
    "Student",
    "Artist",
    "Journalist",
    "Activist",
]

LIST_SOCIAL_MEDIA = [
    "Telegram",
    "VK",
    "Facebook",
    "Instagram",
    "Twitter",
    "YouTube",
    "Other",
]

LIST_PLACES_DETENTION = [
    "Prison",
    "Work Camp",
    "Police Station",
    "Detention Center",
    "Work Camp + Prison",
]
