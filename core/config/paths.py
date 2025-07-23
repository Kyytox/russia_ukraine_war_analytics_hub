###########
## PATHS ##
###########

# root
PATH_ROOT = "/mnt/4046B55946B55080/russia_ukraine_war_analytics_hub"


# credentials
PATH_CREDS_API = "core/utils/credentials.yaml"
PATH_SERVICE_ACCOUNT = "core/utils/token.json"
# PATH_SERVICE_ACCOUNT = "core/utils/creds.json"
PATH_CREDS_GCP = "core/utils/credentials.json"

# data Telegram
PATH_TELEGRAM_RAW = "data/datalake/telegram/raw"
PATH_TELEGRAM_CLEAN = "data/datalake/telegram/clean"
PATH_TELEGRAM_TRANSFORM = "data/datalake/telegram/transform"
PATH_TELEGRAM_FILTER = "data/datalake/telegram/filtered"

# data Twitter
PATH_TWITTER_RAW = "data/datalake/twitter/raw"
PATH_TWITTER_CLEAN = "data/datalake/twitter/clean"
PATH_TWITTER_FILTER = "data/datalake/twitter/filtered"

# Data Filter
PATH_FILTER_DATALAKE = "data/datalake/filter"

# Data Qualif
PATH_QUALIF_DATALAKE = "data/datalake/qualification"

# Data Classify
PATH_CLASSIFY_DATALAKE = "data/datalake/classify"


# Data Wharehouse
PATH_DWH_SOURCES = "data/data_warehouse/sources"

# Data Marts
PATH_DMT_INC_RAILWAY = "data/data_warehouse/datamarts/incidents_railway"
PATH_DMT_RU_BLOCK_SITES = "data/data_warehouse/datamarts/russia_block_sites"
PATH_DMT_COMPO_WEAPONS = "data/data_warehouse/datamarts/compo_weapons"

# Path Utils
PATH_JSON_RU_REGION = "core/utils/ru_region.json"
PATH_COUNTRY_ISO = "core/config/countries_iso.json"

# scripts
PATH_SCRIPT_SERVICE_OLLAMA = "core/utils/script_service.sh"
