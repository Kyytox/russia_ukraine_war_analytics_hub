# Variables for the project


#
#
#
#
#
#
#
#

list_accounts_telegram = [
    "electrichki",
    "astrapress",
    "shot_shot",
]


##########
## PATH ##
##########

# credentials
credentials_path = "code/utils/credentials.yaml"

# data
path_telegram_raw = "data/social_media/telegram/raw"
path_telegram_clean = "data/social_media/telegram/clean"

#
#
#
#
#
#
#
##
#

###############
## VARIABLES ##
###############

dict_utc = {
    "astrapress": 2,
    "shot_shot": 3,
    "electrichki": 3,
}

# text to translate
size_to_translate = 39

prefix_message_ia = "Translate Russian to English: "


list_words_set_railway = [
    {"train", "fire"},
    {"train", "collision"},
    {"train", "derail"},
    {"train", "derailment"},
    {"train", "collided"},
    {"train", "explosion"},
    {"train", "sabotage"},
    {"train", "attack"},
    {"train", "arson"},
    {"train", "destroy"},
    {"train", "damage"},
    {"train", "diversion"},
    {"train", "blow"},
    {"train", "accident"},
    {"train", "accidents"},
    #
    {"trains", "fire"},
    {"trains", "collision"},
    {"trains", "derail"},
    {"trains", "derailment"},
    {"trains", "collided"},
    {"trains", "explosion"},
    {"trains", "sabotage"},
    {"trains", "attack"},
    {"trains", "arson"},
    {"trains", "destroy"},
    {"trains", "damage"},
    {"trains", "diversion"},
    {"trains", "blow"},
    {"trains", "accident"},
    {"trains", "accidents"},
]

list_substr_set_railway = [
    {"sabotage", "railway"},
    {"sabotage", "railroad"},
    {"sabotage", "station"},
    {"sabotage", "rail"},
    {"sabotage", "freight"},
    {"sabotage", "relay"},
    {"sabotage", "accumulator"},
    {"sabotage", "electric"},
    {"sabotage", "battery"},
    #
    {"relay", "box"},
    {"relay", "room"},
    {"relay", "cabins"},
    {"relay", "cabinet"},
    #
    {"cabinet", "accumulator"},
    {"cabinet", "electric"},
    {"cabinet", "electrical"},
    {"cabinet", "battery"},
    #
    {"railway", "bombing"},
    {"railway", "blow"},
    {"railway", "explosion"},
    {"railway", "damage"},
    {"railway", "accident"},
    #
    {"railroad", "bombing"},
    {"railroad", "blow"},
    {"railroad", "explosion"},
    {"railroad", "damage"},
    {"railroad", "accident"},
    #
    {"arson", "relay"},
    {"arson", "railway"},
    {"arson", "cabinet"},
    {"arson", "substation"},
    {"arson", "freight"},
    {"arson", "locomotive"},
    #
    {"fire", "relay"},
    {"fire", "cabinet"},
    {"fire", "substation"},
    #
    {"diversion", "railway"},
    {"diversion", "railroad"},
    #
    {"damage", "railway"},
    {"damage", "railroad"},
    #
    {"collision", "freight"},
    #
    {"wagons", "derail"},
    {"car", "derail"},
    #
    {"locomotive", "fire"},
    {"locomotive", "explosion"},
    {"locomotive", "collision"},
    {"locomotive", "derail"},
    {"locomotive", "damage"},
    {"locomotive", "sabotage"},
    {"locomotive", "destroy"},
    {"locomotive", "attack"},
    {"locomotive", "accident"},
    #
    {"rail", "fire"},
    {"rail", "explosion"},
    {"rail", "collision"},
    {"rail", "derail"},
    {"rail", "damage"},
    {"rail", "sabotage"},
    {"rail", "accident"},
    #
    {"freight", "fire"},
    {"freight", "explosion"},
    {"freight", "collision"},
    {"freight", "derail"},
    {"freight", "damage"},
    {"freight", "sabotage"},
    {"freight", "accident"},
    #
    {"destroy", "railway"},
    {"destroy", "railroad"},
    {"destroy", "relay"},
    {"destroy", "cabinet"},
    {"destroy", "substation"},
    #
    {"attack", "railway"},
    {"attack", "railroad"},
    {"attack", "relay"},
    {"attack", "cabinet"},
    {"attack", "substation"},
    #
    {"track", "damage"},
    {"track", "sabotage"},
    {"track", "fire"},
    {"track", "explosion"},
    {"track", "collision"},
    {"track", "derail"},
    {"track", "destroy"},
    {"track", "attack"},
    #
    {"carriage", "fire"},
    {"carriage", "explosion"},
    {"carriage", "collision"},
    {"carriage", "derail"},
    {"carriage", "damage"},
    {"carriage", "sabotage"},
    {"carriage", "destroy"},
    {"carriage", "attack"},
]


# list of words referring to railroads or railroad incidents
list_en_expression_railways = [
    "relay box",
    "relay boxes",
    "relay room",
    "relay cabins",
    "relay cabinet",
    "relay cabinets",
    "accumulator cabinet",
    "accumulator cabinets",
    "electrical cabinet",
    "electrical cabinets",
    "electric cabinet",
    "electric cabinets",
    "railway bombing",
    "battery cabinet",
    "battery cabinets",
    "railway sabotage",
    "railway explosion",
    "railway track damage",
    "railway was blown up",
    "railway tracks damage",
    "railway diversion",
    "railway station",
    "railway blast",
    "wagons derailed",
    "wagons derailment",
    "train fire",
    "train sabotage",
    "train collision",
    "train collided",
    "train derailment",
    "train equipment fire",
    "arson attack",
    "arson on railway",
    "arson on railway",
    "arson on the railway",
    "arson of relay",
    "fire to railway",
    "fire to a cabinet",
    "fire to a cabinets",
    "fire to the cabinet",
    "fire to the cabinets",
    "fire to a substation",
    "fire at a railway",
    "sabotage on the railway",
    "sabotage on railway",
    "sabotage of railway",
    "sabotage at a railroad",
    "diversion on railway",
    "diversions on railway",
    "diversions on the railway",
    "diversion on the railroad",
    "diversions on the railroad",
    "damage to railway",
    "damage to railroad",
    "damaged railway",
    "damaged railroad",
    "damaging railway",
    "damaging railroad",
    "blowing up railway",
    "blowing up railroad",
    "blow up railway",
    "blow up railroad",
    "railroad was blown up",
    "cars derailed",
    "set fire to relay",
    "destroy railroad",
    "rail sabotage",
    "attacks on railroad",
    "attacked a traction",
    "attacked a railway",
    "attacked a railroad",
    "attacks on the railway",
    "explosion of railway",
]

# N° article of justice for incidents
list_en_words_railways = [
    "167",
    "205",
    "205.5",
    "205-205.5",
    "267",
    "275",
    "275.1",
    "275-275.1",
    "281",
]