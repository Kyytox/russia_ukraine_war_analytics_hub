###########
## PATHS ##
###########

# root
PATH_ROOT = "/home/kytox/dev/russia_ukraine_war_analytics_hub"
PATH_DATA = "data"
# PATH_DATA = "data_dev"  # dev

# credentials
PATH_CREDS_API = "core/utils/credentials.yaml"
PATH_SERVICE_ACCOUNT = "core/utils/token.json"
# PATH_SERVICE_ACCOUNT = "core/utils/creds.json"
PATH_CREDS_GCP = "core/utils/credentials.json"

# data Telegram
PATH_TELEGRAM_RAW = f"{PATH_DATA}/datalake/telegram/raw"
PATH_TELEGRAM_CLEAN = f"{PATH_DATA}/datalake/telegram/clean"
PATH_TELEGRAM_TRANSFORM = f"{PATH_DATA}/datalake/telegram/transform"
PATH_TELEGRAM_FILTER = f"{PATH_DATA}/datalake/telegram/filtered"

# data Twitter
PATH_TWITTER_RAW = f"{PATH_DATA}/datalake/twitter/raw"
PATH_TWITTER_CLEAN = f"{PATH_DATA}/datalake/twitter/clean"
PATH_TWITTER_FILTER = f"{PATH_DATA}/datalake/twitter/filtered"

# Data Filter
PATH_FILTER_DATALAKE = f"{PATH_DATA}/datalake/filter"

# Data Qualif
PATH_QUALIF_DATALAKE = f"{PATH_DATA}/datalake/qualification"
PATH_DB_QUALIF_DATALAKE = f"{PATH_DATA}/datalake/qualification/dlk_qualification.db"

# Data Classify
PATH_CLASSIFY_DATALAKE = f"{PATH_DATA}/datalake/classify"

# Data Wharehouse
PATH_DWH_SOURCES = f"{PATH_DATA}/data_warehouse/sources"
PATH_DWH_SOURCES_RU_OFFICERS_KIU = f"{PATH_DATA}/data_warehouse/sources/ru_officers_kiu"
PATH_DWH_MILITARY_LOSSES = f"{PATH_DATA}/data_warehouse/sources/military_losses"

# Data Marts
PATH_DMT_INC_RAILWAY = f"{PATH_DATA}/data_warehouse/datamarts/incidents_railway"
PATH_DMT_RU_BLOCK_SITES = f"{PATH_DATA}/data_warehouse/datamarts/russia_block_sites"
PATH_DMT_COMPO_WEAPONS = f"{PATH_DATA}/data_warehouse/datamarts/compo_weapons"
PATH_DMT_RU_OFFICERS_KIU = f"{PATH_DATA}/data_warehouse/datamarts/ru_officers_kiu"

# Path Utils
PATH_JSON_RU_REGION = "core/utils/ru_region.json"
PATH_COUNTRY_ISO = "core/config/countries_iso.json"

# scripts
PATH_SCRIPT_SERVICE_OLLAMA = "core/utils/script_service.sh"
