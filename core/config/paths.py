###########
## PATHS ##
###########

# root
PATH_ROOT = "/mnt/4046B55946B55080/russia_ukraine_war_analytics_hub"


# credentials
PATH_CREDS_API = "core/utils/credentials.yaml"
PATH_CREDS_GOOGLE_SHEET = "core/utils/credentials.json"
PATH_TOKEN_GOOGLE_SHEET = "core/utils/token.json"

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
# PATH_DWH_TRANSFORM = "data/data_warehouse/transform"

# Data Marts
# PATH_DMT_INC_RAILWAY = "data/datamarts/incidents_railway"
PATH_DMT_INC_RAILWAY = "data/data_warehouse/datamarts/incidents_railway"

# Path Utils
# PATH_JSON_RU_REGION = "core/utils/russia_region.json"
PATH_JSON_RU_REGION = "core/utils/ru_region.json"

# scripts
PATH_SCRIPT_SERVICE_OLLAMA = "core/utils/script_service.sh"
