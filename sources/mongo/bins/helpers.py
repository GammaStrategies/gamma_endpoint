from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import (
    database_global,
    database_local,
    database_perps,
    database_xtrade,
)

# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL

# General database helpers


def local_database_helper(network: Chain):
    """Create a local database for a hypervisor."""
    return database_local(
        mongo_url=MONGO_DB_URL,
        db_name=f"{network.database_name}_gamma",
    )


def global_database_helper():
    """Create a global database."""
    return database_global(mongo_url=MONGO_DB_URL)


def perps_database_helper():
    """Create a perps database."""
    return database_perps(mongo_url=MONGO_DB_URL, db_name="perps_gamma")


def xtrade_database_helper():
    """xtrade database helper"""
    return database_xtrade(mongo_url=MONGO_DB_URL, db_name="xlayer_xtrade")
