#
# Correlation
#
import asyncio
import time
import numpy as np

import pandas as pd
from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import global_database_helper, local_database_helper
from sources.subgraph.bins.constants import BLOCK_TIME_SECONDS


async def get_correlation(
    chains: list[Chain] = None,
    token_addresses: list[str] = None,
    from_timestamp: int = None,
    to_timestamp: int = None,
):
    df = convert_to_dataframe(
        await get_prices(
            chains=chains,
            token_addresses=token_addresses,
            from_block=1,
            group_blocks=100000,
        )
    )
    df = df.set_index(keys="timestamp", append=True, verify_integrity=True, drop=True)
    df = df.groupby("timestamp", as_index=True, sort=True).last()

    # calculate correlation ( expanding data from 0 to xx)
    correlation = df.expanding(10).corr(pairwise=True)
    # remove NaN and inf
    correlation = correlation[~correlation.isin([np.nan, np.inf, -np.inf]).any(axis=1)]

    result = []
    for idx, row in correlation.iterrows():
        # create item
        _itm = {"timestamp": idx[0]}
        # loop through token addresses and add them to item
        for i in range(0, len(token_addresses)):
            _itm[token_addresses[i]] = row[token_addresses[i]]

        # add to result
        result.append(_itm)

    return result


async def get_correlation_from_hypervisors(
    chain: Chain,
    hypervisor_addresses: list[str],
    from_timestamp: int = None,
    to_timestamp: int = None,
) -> dict:
    if len(hypervisor_addresses) == 1:
        return await get_correlation_from_one_token_pair(
            chain=chain,
            token_addresses=await get_hypervisor_tokens(
                chain=chain, hypervisor_addresses=hypervisor_addresses
            ),
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )

    # get data
    return await get_correlation(
        chains=[chain],
        token_addresses=await get_hypervisor_tokens(
            chain=chain, hypervisor_addresses=hypervisor_addresses
        ),
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
    )


async def get_correlation_from_one_token_pair(
    chain: Chain,
    token_addresses: [str, str],
    from_timestamp: int = None,
    to_timestamp: int = None,
):
    """Get correlation from one token pair only

    Args:
        chain (Chain): _description_
        token0_addres (str): _description_
        token1_address (str): _description_
        from_timestamp (int, optional): _description_. Defaults to None.
        to_timestamp (int, optional): _description_. Defaults to None.   0x6b7635b7d2e85188db41c3c05b1efa87b143fce8

    """

    # define blocks to search for
    from_block, to_block, _group_blocks = await convert_to_blocks(
        chain=chain, from_timestamp=from_timestamp, to_timestamp=to_timestamp
    )

    # build dataFrame
    df = convert_to_dataframe(
        await get_prices(
            chains=[chain],
            token_addresses=token_addresses,
            from_block=from_block,
            to_block=to_block,
            group_blocks=_group_blocks,
        )
    )

    # if empty return empty list
    if df.empty:
        return []

    df = df.set_index(keys="timestamp", append=True, verify_integrity=True, drop=True)
    df = df.groupby("timestamp", as_index=True, sort=True).last()

    # calculate correlation ( expanding data from 0 to xx)
    correlation = df.expanding(10).corr(pairwise=True)
    # remove NaN and inf
    correlation = correlation[~correlation.isin([np.nan, np.inf, -np.inf]).any(axis=1)]

    return [
        {
            "timestamp": idx[0],
            token_addresses[0]: round(row[token_addresses[0]], 2),
            token_addresses[1]: round(row[token_addresses[1]], 2),
        }
        for idx, row in correlation.iterrows()
        if idx[-1] == token_addresses[0]
    ]


# Helper functions


async def convert_to_blocks(
    chain: Chain, from_timestamp: int = None, to_timestamp: int = None
) -> tuple[int, int, int]:
    """Convert timestamp to blocks

    Args:
        chain (Chain): _description_
        from_timestamp (int, optional): _description_. Defaults to None.
        to_timestamp (int, optional): _description_. Defaults to None.   0x6b7635b7d2e85188db41c3c05b1efa87b143fce8

    Returns:
        tuple[int,int,int]: from_block, to_block, group_blocks
    """

    # default group_blocks
    _seconds_block = BLOCK_TIME_SECONDS.get(chain, 5)
    _blocks_hour = 3600 / _seconds_block
    # define a max group_blocks
    group_blocks = _blocks_hour * 116

    _timestamp = {}
    if from_timestamp:
        _timestamp["$gte"] = from_timestamp

        # calc _group_blocks
        _days_period = ((to_timestamp or int(time.time())) - from_timestamp) / (
            3600 * 24
        )
        _seconds_block = BLOCK_TIME_SECONDS.get(chain, 5)
        _blocks_hour = 3600 / _seconds_block

        group_blocks = (
            group_blocks
            if _days_period >= 180
            else _blocks_hour * 72
            if _days_period >= 60
            else _blocks_hour * 48
            if _days_period >= 30
            else _blocks_hour * 24
            if _days_period >= 14
            else _blocks_hour * 3
            if _days_period >= 7
            else _blocks_hour
        )

    if to_timestamp:
        _timestamp["$lte"] = to_timestamp
    else:
        _timestamp["$lte"] = from_timestamp + (3600 * 24)
        to_block = None
    #
    if _timestamp:
        _data = await global_database_helper().get_items_from_database(
            collection_name="blocks",
            find={"timestamp": _timestamp, "network": chain.database_name},
            sort=[("timestamp", 1)],
        )
        from_block = _data[0]["block"]
        if to_timestamp:
            to_block = _data[-1]["block"]
    else:
        from_block = None
        to_block = None

    return from_block, to_block, group_blocks


async def get_hypervisor_tokens(chain: Chain, hypervisor_addresses: list[str]) -> dict:
    # get hypervisor token addresses
    result = []
    for hypervisor_static in await local_database_helper(
        network=chain
    ).get_items_from_database(
        collection_name="static", find={"address": {"$in": hypervisor_addresses}}
    ):
        result.append(hypervisor_static["pool"]["token0"]["address"])
        result.append(hypervisor_static["pool"]["token1"]["address"])

    return result


async def get_prices(
    chains: list[Chain],
    token_addresses: list[str],
    from_block: int,
    to_block: int | None = None,
    group_blocks: int = 100,
) -> list[dict]:
    """Get a list of prices from the database

    Args:
        chains (list[Chain], optional): . Defaults to None.
        token_addresses (list[str], optional): . Defaults to None.
        limit (int, optional): . Defaults to None.

    Returns:
        list[dict]:
    """

    # create match
    _match = {}
    _match["network"] = {"$in": [chain.database_name for chain in chains]}
    _match["address"] = {"$in": token_addresses}
    _match["block"] = {"$gte": from_block}
    if to_block:
        _match["block"]["$lte"] = to_block
    # build query
    query = [
        {
            "$group": {
                "_id": {"block": "$block", "network": "$network"},
                "items": {"$push": {"address": "$address", "price": "$price"}},
                "count": {"$sum": 1},
                "block_group": {
                    "$first": {
                        "$subtract": ["$block", {"$mod": ["$block", group_blocks]}]
                    }
                },
            }
        },
        {"$match": {"count": {"$gt": 1}}},
        {
            "$group": {
                "_id": "$block_group",
                "network": {"$first": "$_id.network"},
                "block": {"$first": "$_id.block"},
                "items": {"$first": "$items"},
            }
        },
        {"$limit": 8000},
        {"$sort": {"block": 1}},
        {
            "$lookup": {
                "from": "blocks",
                "let": {"op_network": "$network", "op_block": "$block"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$block", "$$op_block"]},
                                    {"$eq": ["$network", "$$op_network"]},
                                ]
                            }
                        }
                    }
                ],
                "as": "timestamp",
            }
        },
        {"$addFields": {"timestamp": {"$first": "$timestamp.timestamp"}}},
    ]

    # add first match
    if _match:
        query.insert(0, {"$match": _match})

    # get prices
    return await global_database_helper().get_items_from_database(
        collection_name="usd_prices", aggregate=query
    )


def convert_to_dataframe(data: list[dict]) -> pd.DataFrame:
    """Convert a list of dictionaries to a pandas dataframe

    Args:
        data (list[dict]): list of dictionaries, dict items being:
                {   _id:{ block: 16669913, network: "ethereum" }
                    count: 2 or more
                    items: [...2 or more items...]
                    timestamp:170669913
                }


    Returns:
        pd.DataFrame: pandas dataframe
    """

    return pd.DataFrame.from_dict(
        [
            {
                "timestamp": x["timestamp"],
                x["items"][0]["address"]: x["items"][0]["price"],
                x["items"][1]["address"]: x["items"][1]["price"],
            }
            for x in data
        ]
    )
