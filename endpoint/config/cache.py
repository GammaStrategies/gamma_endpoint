from endpoint.config import get_config


CHARTS_CACHE_TIMEOUT = int(get_config("CHARTS_CACHE_TIMEOUT"))

APY_CACHE_TIMEOUT = int(get_config("APY_CACHE_TIMEOUT"))

DASHBOARD_CACHE_TIMEOUT = int(get_config("DASHBOARD_CACHE_TIMEOUT"))

ALLDATA_CACHE_TIMEOUT = int(get_config("ALLDATA_CACHE_TIMEOUT"))

USER_CACHE_TIMEOUT = int(get_config("USER_CACHE_TIMEOUT"))

DB_CACHE_TIMEOUT = int(get_config("DB_CACHE_TIMEOUT"))  # database calls cache

# data needed to be refreshed every once a day
DAILY_CACHE_TIMEOUT = int(get_config("DAILY_CACHE_TIMEOUT"))

LONG_CACHE_TIMEOUT = int(get_config("LONG_CACHE_TIMEOUT"))
