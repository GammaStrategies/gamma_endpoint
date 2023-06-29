import asyncio
from sources.common.general.enums import Chain, Protocol

# TODO: restruct global config and local config
from sources.mongo.bins.apps.hypervisor import local_database_helper


async def tvl():
    result = {}
    # for chain in Chain:
    #     result[chain.name] = await local_database_helper(network=chain).get_items_from_database(
    #             collection_name="status",
    #             find={"dex": protocol.database_name},
    #             projection={"_id": 0},
    #         )
