import asyncio
import logging
import time
from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import global_database_helper
from sources.subgraph.bins.constants import BLOCK_TIME_SECONDS


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
            for item in await global_database_helper().get_items_from_database(
                collection_name="usd_prices", find=find
            )
        }
    elif not token_addresses and not block:
        # return last available prices for all known tokens ( within the last 10000 scraped prices in the nework)
        return {
            item["address"]: {"price": item["price"], "block": item["block"]}
            for item in await global_database_helper().get_prices_usd_last(
                network=network.database_name
            )
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
            global_database_helper().get_items_from_database(
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

    return await global_database_helper().get_items_from_database(
        collection_name="current_usd_prices",
        find=find,
        projection={"_id": False, "id": False},
        batch_size=50000,
    )


async def get_database_prices_closeto(
    chain: Chain,
    timestamp: int | None = None,
    block: int | None = None,
    default_to_current: bool = True,
) -> list[dict]:
    """Return the closest prices to the period

    Args:
        chain (Chain):
        end_timestamp (int | None, optional): . Defaults to None.
        end_block (int | None, optional): . Defaults to None.
        threshold (int, optional): blocks before the specified to be considered close. Defaults to 10000.
        default_to_current (bool, optional): If errors occur, default to current prices. Defaults to True.

    Returns:
        list[dict]:  {address:{
            "address": str,
            "block": int,
            "price": float,
            "timestamp": int,
            "network": str
        }, ...}
    """

    # get hypervisors prices at the end of the period
    token_prices = {}
    try:
        if timestamp or block:
            # define end block
            _db_end_block = block
            if not _db_end_block:
                # get a list of blocks close to the end of the period
                _db_end_block = await global_database_helper().get_closest_block(
                    network=chain.database_name, timestamp=timestamp
                )
                # assign the first block of the list
                _db_end_block = _db_end_block[0]["doc"]["block"]
                logging.getLogger(__name__).debug(
                    f" using database proces closest block: {_db_end_block}"
                )
                _initial_block = _db_end_block - (
                    60 * 60 * 24 * 30 * 4 / BLOCK_TIME_SECONDS.get(chain, 10)
                )
                if _initial_block < _db_end_block:
                    _initial_block = int(_db_end_block * 0.92)

            # build and_query part
            _and_query = [
                {"network": chain.database_name},
                {"block": {"$lte": _db_end_block, "$gte": _initial_block}},
            ]
            # build query
            query = [
                {"$match": {"$and": _and_query}},
                {"$sort": {"block": -1}},
                {"$group": {"_id": "$address", "last": {"$first": "$$ROOT"}}},
            ]
            token_prices = {
                x["last"]["address"]: x["last"]
                for x in await global_database_helper().get_items_from_database(
                    collection_name="usd_prices", aggregate=query
                )
            }
    except Exception as e:
        logging.getLogger(__name__).error(
            f" Cant get token prices for {chain} at {timestamp or block}. Error: {e}"
        )

    if not token_prices and default_to_current:
        logging.getLogger(__name__).warning(
            f" Using current prices to get fees for {chain}."
        )
        # get hypervisors current prices
        token_prices = {
            x["address"]: x for x in await get_current_prices(network=chain)
        }

    return token_prices
