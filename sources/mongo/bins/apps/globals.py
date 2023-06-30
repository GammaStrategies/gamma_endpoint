import asyncio
from sources.common.general.enums import Chain, Protocol

# TODO: restruct global config and local config
from sources.mongo.bins.apps.hypervisor import local_database_helper


async def tvl():
    _queries = []
    # for chain in Chain:
    #     #_queries.append(
    # #     result[chain.name] = await local_database_helper(network=chain).get_items_from_database(
    # #             collection_name="status",
    # #             find={"dex": protocol.database_name},
    # #             projection={"_id": 0},
    # #         )

    #     find = {
    #         "network": network.database_name,
    #         "address": address,
    #     }
    #     if block:
    #         find["block"] = block

    #     _queries.append(
    #         database_global(mongo_url=MONGO_DB_URL).get_items_from_database(
    #             collection_name="usd_prices",
    #             find=find,
    #             sort=[("block", -1)],
    #             limit=1,
    #         )
    #     )

    # # asyncio gather all queries and return price per address
    # return {
    #     item["address"]: {"price": item["price"], "block": item["block"]}
    #     for items in await asyncio.gather(*price_queries)
    #     for item in items
    # }


async def tokens():
    pass


async def hypervisors():
    pass


async def pools():
    pass


async def users():
    pass
