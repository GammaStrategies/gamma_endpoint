#
# Correlation
#
import asyncio

import pandas as pd
from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import global_database_helper, local_database_helper


async def get_correlation_from_hypervisors(
    chain: Chain, hypervisor_addresses: list[str]
) -> dict:
    # get data
    return await get_correlation(
        chains=[chain],
        token_addresses=await get_hypervisor_tokens(
            chain=chain, hypervisor_addresses=hypervisor_addresses
        ),
    )


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


async def get_correlation(
    chains: list[Chain] = None, token_addresses: list[str] = None, limit: int = 6000
):
    df = convert_to_dataframe(
        await get_prices(chains=chains, token_addresses=token_addresses, limit=limit)
    )
    df = df.set_index(keys="block", append=True, verify_integrity=True, drop=True)
    df = df.groupby("block", as_index=True, sort=False).last()
    correlation = df.corr(method="pearson").fillna("no data")
    return correlation.to_dict()


async def get_prices(
    chains: list[Chain] = None, token_addresses: list[str] = None, limit: int = None
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
    if chains:
        _match["network"] = {"$in": [chain.database_name for chain in chains]}
    if token_addresses:
        _match["address"] = {"$in": token_addresses}
    # build query
    query = [
        {
            "$group": {
                "_id": "$block",
                "items": {
                    "$push": {
                        "block": "$block",
                        "network": "$network",
                        "address": "$address",
                        "price": "$price",
                    }
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id": -1}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    if limit:
        query.append({"$limit": limit})
    # add match
    if _match:
        query.insert(0, {"$match": _match})

    # sort by block
    query.append({"$sort": {"_id": 1}})

    # # unwind
    # query.append({"$unwind": {"path": "$items"}})
    # # project
    # query.append(
    #     {
    #         "$project": {
    #             "_id": 0,
    #             "block": "$items.block",
    #             "network": "$items.network",
    #             "address": "$items.address",
    #             "price": "$items.price",
    #         }
    #     }
    # )

    # get prices
    return await global_database_helper().get_items_from_database(
        collection_name="usd_prices", aggregate=query
    )


# async def get_prices(
#     chains: list[Chain] = None, token_addresses: list[str] = None, limit: int = None
# ) -> list[dict]:
#     """Get a list of prices from the database

#     Args:
#         chains (list[Chain], optional): . Defaults to None.
#         token_addresses (list[str], optional): . Defaults to None.
#         limit (int, optional): . Defaults to None.

#     Returns:
#         list[dict]:
#     """
#     # create match
#     _match = {}
#     if chains:
#         _match["network"] = {"$in": [chain.database_name for chain in chains]}
#     if token_addresses:
#         _match["address"] = {"$in": token_addresses}
#     # build query
#     query = [
#         {"$match": _match},
#         {"$sort": {"block": -1}},
#     ]
#     if limit:
#         query.append({"$limit": limit})
#     # add match
#     if _match:
#         query.insert(0, {"$match": _match})

#     query.append({"$sort": {"block": 1}})
#     # query.append({"$project": {"_id": 1}})

#     # get prices
#     return await global_database_helper().get_items_from_database(
#         collection_name="usd_prices", aggregate=query
#     )


def convert_to_dataframe(data: list[dict]) -> pd.DataFrame:
    """Convert a list of dictionaries to a pandas dataframe

    Args:
        data (list[dict]): list of dictionaries

    Returns:
        pd.DataFrame: pandas dataframe
    """

    return pd.DataFrame.from_dict(
        [
            {
                "block": x["_id"],
                x["items"][0]["address"]: x["items"][0]["price"],
                x["items"][1]["address"]: x["items"][1]["price"],
            }
            for x in data
        ]
    )
