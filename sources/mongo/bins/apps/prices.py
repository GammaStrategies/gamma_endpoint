import asyncio
from sources.common.database.collection_endpoint import database_global
from sources.common.general.enums import Chain
from sources.subgraph.bins.config import MONGO_DB_URL


async def get_prices(
    network: Chain,
    token_addresses: list[str] | None = None,
    block: int | None = None,
) -> dict:
    """Get a list of token prices from database

    Args:
        token_addresses (list[str]): token address list ( lower case)
        block (int): block number
        network (Chain):

    Returns:
        dict: with token 'address' as key and 'price' as value
    """

    if not token_addresses and block:
        # return all available prices at block
        find = {
            "network": network.database_name,
            "block": block,
        }
        return {
            item["address"]: {"price": item["price"], "block": item["block"]}
            for item in await database_global(
                mongo_url=MONGO_DB_URL
            ).get_items_from_database(collection_name="usd_prices", find=find)
        }
    elif not token_addresses and not block:
        # return last available prices for all known tokens ( within the last 10000 scraped prices in the nework)
        return {
            item["address"]: {"price": item["price"], "block": item["block"]}
            for item in await database_global(
                mongo_url=MONGO_DB_URL
            ).get_prices_usd_last(network=network.database_name)
        }

    # return prices for the given token addresses at the given block (or last available block)
    price_queries = []
    for address in token_addresses:
        find = {
            "network": network.database_name,
            "address": address,
        }
        if block:
            find["block"] = block

        price_queries.append(
            database_global(mongo_url=MONGO_DB_URL).get_items_from_database(
                collection_name="usd_prices",
                find=find,
                sort=[("block", -1)],
                limit=1,
            )
        )

    # asyncio gather all queries and return price per address
    return {
        item["address"]: {"price": item["price"], "block": item["block"]}
        for items in await asyncio.gather(*price_queries)
        for item in items
    }


async def get_current_prices(
    network: Chain, token_addresses: list[str] | None = None
) -> list[dict]:
    find = {"network": network.database_name}
    if token_addresses:
        # build ids
        ids = [f"{network.database_name}_{address}" for address in token_addresses]
        find = {"id": {"$in": ids}}
    else:
        find = {"network": network.database_name}

    return await database_global(mongo_url=MONGO_DB_URL).get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
        projection={"_id": False, "id": False},
        batch_size=50000,
    )
