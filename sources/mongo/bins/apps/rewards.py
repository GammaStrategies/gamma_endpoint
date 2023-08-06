import asyncio
from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import database_global, database_local
from sources.mongo.bins.apps.hypervisor import local_database_helper


# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


# MultiFeeDistributor


async def latest_multifeeDistributor(network: Chain, protocol: Protocol):
    # return await local_database_helper(network=network).get_items_from_database(
    #     collection_name="latest_multifeedistribution",
    #     find={"dex": protocol.database_name},
    #     projection={"_id": 0},
    # )
    return await local_database_helper(network=network).get_latest_multifeedistribution(
        dex=protocol.database_name
    )
