import asyncio
from datetime import datetime, timezone
from sources.common.database.collection_endpoint import database_global
from sources.common.general.enums import Chain, text_to_chain
from sources.subgraph.bins.config import MONGO_DB_URL


async def get_current_prices(
    chain: Chain | None = None, token_addresses: list[str] | None = None
) -> list[dict]:
    """Get a list of current token prices from database

    Args:
        network (Chain): _description_
        token_addresses (list[str] | None, optional): . Defaults to None.

    Returns:
        list[dict]:
    """
    find = {}
    if chain:
        find = {"network": chain.database_name}
    if token_addresses:
        # build ids
        ids = [f"{chain.database_name}_{address}" for address in token_addresses]
        find["id"] = {"$in": ids}

    result = []
    current_time = datetime.now(timezone.utc).timestamp()

    for item in await database_global(mongo_url=MONGO_DB_URL).get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
        sort=[("network", 1)],
        projection={"_id": False, "id": False},
        batch_size=50000,
    ):
        try:
            item["seconds_old"] = current_time - item["timestamp"]
        except:
            item["seconds_old"] = None

        # change string network name to Chain object
        try:
            chain = text_to_chain(item["network"])
            item["network"] = chain.subgraph_name
        except:
            pass
        result.append(item)

    return result


async def get_current_token_addresses(chain: Chain | None = None) -> list[list]:
    """comma separated list of current token addresses from database

    Args:
        chain (Chain):

    Returns:
        list[dict]:
    """
    find = {}
    if chain:
        find = {"network": chain.database_name}

    result = []
    for item in await database_global(mongo_url=MONGO_DB_URL).get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
        sort=[("network", 1)],
        projection={"_id": False, "id": False},
        batch_size=50000,
    ):
        result.append([item["address"], item["network"]])
    return result
