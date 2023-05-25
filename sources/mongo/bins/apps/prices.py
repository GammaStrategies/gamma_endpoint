import asyncio
from sources.common.database.collection_endpoint import database_global
from sources.common.general.enums import Chain
from sources.subgraph.bins.config import MONGO_DB_URL


async def get_prices(
    token_addresses: list[str],
    network: Chain,
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
