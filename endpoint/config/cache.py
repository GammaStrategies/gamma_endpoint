from endpoint.config import get_config


CHARTS_CACHE_TIMEOUT = int(get_config("CHARTS_CACHE_TIMEOUT"))

APY_CACHE_TIMEOUT = int(get_config("APY_CACHE_TIMEOUT"))

DASHBOARD_CACHE_TIMEOUT = int(get_config("DASHBOARD_CACHE_TIMEOUT"))

ALLDATA_CACHE_TIMEOUT = int(get_config("ALLDATA_CACHE_TIMEOUT"))

DB_CACHE_TIMEOUT = int(get_config("DB_CACHE_TIMEOUT"))  # database calls cache

