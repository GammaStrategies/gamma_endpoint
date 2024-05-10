#
#


import asyncio
from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import global_database_helper, local_database_helper


async def get_user_addresses(
    chain: Chain, hypervisor_address: str | None = None
) -> list[dict]:
    """User addresses known.

    Args:
        chain (Chain): chain to query

    Returns:
        list[dict]: list
    """
    return await local_database_helper(network=chain).get_items_from_database(
        collection_name="operations",
        aggregate=query_all_user_addresses(hypervisor_address=hypervisor_address),
    )


async def get_user_shares(
    user_address: str,
    chain: Chain,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
    include_operations: bool = False,
) -> list[dict]:
    """User shares at a specific time ( default is last known)."""
    return [
        global_database_helper().convert_d128_to_decimal(x)
        for x in await local_database_helper(network=chain).get_items_from_database(
            collection_name="operations",
            aggregate=query_user_shares_merkl(
                user_address=user_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
                block_ini=block_ini,
                block_end=block_end,
                hypervisor_address=hypervisor_address,
                include_operations=include_operations,
            ),
        )
    ]


def query_all_user_addresses(hypervisor_address: str | None = None) -> list[dict]:
    """Query all user addresses from the operations collection.
        Include addresses that received or sent LP tokens anytime.

    Returns:
        list[dict]: list of user addresses
    """
    _query = [
        {"$match": {"topic": "transfer"}},
        {"$project": {"users": ["$src", "$dst"]}},
        {"$unwind": {"path": "$users", "preserveNullAndEmptyArrays": False}},
        {"$match": {"users": {"$ne": "0x0000000000000000000000000000000000000000"}}},
        {"$group": {"_id": "$users"}},
        {"$project": {"user_address": "$_id", "_id": 0}},
    ]

    if hypervisor_address:
        _query[0]["$match"]["address"] = {"$ne": hypervisor_address}

    return _query


def query_user_shares_merkl(
    user_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
    include_operations: bool = False,
):
    """Query to get user shares from the database at a specific time
        ( user_operations collection )

    Args:
        user_address (str): user address to query
        timestamp_ini (int | None, optional): . Defaults to None.
        timestamp_end (int | None, optional): . Defaults to None.
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.
        hypervisor_address (str | None, optional): . Defaults to None.
        include_operations (bool, optional): Include the list of justifying operations ? . Defaults to False.
    """

    # build standard match
    _match = {
        "$and": [
            {
                "$or": [
                    {"src": user_address},
                    {"dst": user_address},
                ]
            }
        ]
    }

    # add timestamp to match if set
    if timestamp_ini and timestamp_end:
        _match["$and"].append(
            {"timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end}}
        )
    elif timestamp_ini:
        _match["$and"].append({"timestamp": {"$gte": timestamp_ini}})
    elif timestamp_end:
        _match["$and"].append({"timestamp": {"$lte": timestamp_end}})
    # add block to match if set
    if block_ini and block_end:
        _match["$and"].append({"blockNumber": {"$gte": block_ini, "$lte": block_end}})
    elif block_ini:
        _match["$and"].append({"blockNumber": {"$gte": block_ini}})
    elif block_end:
        _match["$and"].append({"blockNumber": {"$lte": block_end}})

    # add hypervisor address to match if set
    if hypervisor_address:
        _match["$and"].append({"address": hypervisor_address})

    _group = {
        "_id": "$address",
        "hypervisor": {"$first": "$address"},
        "user_shares": {"$sum": "$shares"},
    }
    _project = {
        "_id": 0,
        "hypervisor": "$hypervisor",
        "timestamp": {"$first": "$hype_status.timestamp"},
        "block": {"$first": "$hype_status.block"},
        "user_shares": "$user_shares",
        "hypervisor_info": {
            "address": "$address",
            "symbol": {"$first": "$hype_status.symbol"},
            "dex": {"$first": "$hype_status.dex"},
            "decimals": {"$first": "$hype_status.decimals"},
            "pool_address": {"$first": "$hype_status.pool_address"},
            "token0_address": {"$first": "$hype_status.token0_address"},
            "token1_address": {"$first": "$hype_status.token1_address"},
            "token0_symbol": {"$first": "$hype_status.token0_symbol"},
            "token1_symbol": {"$first": "$hype_status.token1_symbol"},
            "token0_decimals": {"$first": "$hype_status.token0_decimals"},
            "token1_decimals": {"$first": "$hype_status.token1_decimals"},
        },
        "total_shares": {"$first": "$hype_status.totalSupply"},
        "total_token0": {"$first": "$hype_status.underlying_qtty0"},
        "total_token1": {"$first": "$hype_status.underlying_qtty1"},
    }
    if include_operations:
        _group["operations"] = {
            "$push": {
                "block": "$blockNumber",
                "timestamp": "$timestamp",
                "topic": "$topic",
                "shares": "$shares",
            }
        }
        _project["operations"] = "$operations"

    _query = [
        {"$match": _match},
        {"$sort": {"blockNumber": 1}},
        {
            "$addFields": {
                "shares": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {"$eq": ["$dst", user_address]},
                                {"$toDecimal": {"$ifNull": ["$qtty", "$shares"]}},
                                {"$multiply": [{"$toDecimal": "$qtty"}, -1]},
                            ]
                        },
                        0,
                    ]
                }
            }
        },
        {"$group": _group},
        {
            "$lookup": {
                "from": "status",
                "let": {"op_address": "$hypervisor"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$and": [{"$eq": ["$address", "$$op_address"]}]}
                        }
                    },
                    {"$sort": {"block": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": "$timestamp",
                            "block": "$block",
                            "symbol": "$symbol",
                            "dex": "$dex",
                            "decimals": "$decimals",
                            "pool_address": "$pool.address",
                            "token0_address": "$pool.token0.address",
                            "token1_address": "$pool.token1.address",
                            "token0_symbol": "$pool.token0.symbol",
                            "token1_symbol": "$pool.token1.symbol",
                            "token0_decimals": "$pool.token0.decimals",
                            "token1_decimals": "$pool.token1.decimals",
                            "totalSupply": {"$toDecimal": "$totalSupply"},
                            "underlying_qtty0": {
                                "$sum": [
                                    {"$toDecimal": "$totalAmounts.total0"},
                                    {"$toDecimal": "$fees_uncollected.lps_qtty_token0"},
                                ]
                            },
                            "underlying_qtty1": {
                                "$sum": [
                                    {"$toDecimal": "$totalAmounts.total1"},
                                    {"$toDecimal": "$fees_uncollected.lps_qtty_token1"},
                                ]
                            },
                        }
                    },
                ],
                "as": "hype_status",
            }
        },
        {"$project": _project},
    ]

    return _query


# TODO: in development
def query_user_shares_from_user_operations(
    user_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
) -> list[dict]:

    # block ini and block end or timestamp ini and timestamp end should be set together
    if (block_ini or block_end) and not (block_ini and block_end):
        raise ValueError("block_ini and block_end should be set together")
    if (timestamp_ini or timestamp_end) and not (timestamp_ini and timestamp_end):
        raise ValueError("timestamp_ini and timestamp_end should be set together")
    # any of the block or timestamp should be set
    if not (block_ini or block_end or timestamp_ini or timestamp_end):
        raise ValueError(
            "block_ini, block_end, timestamp_ini or timestamp_end should be set"
        )

    _and_first_match = []
    _first_shares_balance_lt = []
    _operations_match = []

    # user address
    if user_address:
        _and_first_match.append({"user_address": user_address})

    # add hypervisor address to match if set
    if hypervisor_address:
        _and_first_match.append({"address": hypervisor_address})

    if _and_first_match:
        _and_first_match = [{"$match": {"$and": _and_first_match}}]

    # add timestamp to match if set
    if timestamp_ini and timestamp_end:
        _first_shares_balance_lt = ["$timestamp", timestamp_ini]
        _operations_match.append({"$gte": ["$timestamp", timestamp_ini]})
        _operations_match.append({"$lte": ["$timestamp", timestamp_end]})
    elif timestamp_ini:
        _first_shares_balance_lt = ["$timestamp", timestamp_ini]
        _operations_match.append({"$gte": ["$timestamp", timestamp_ini]})
    elif timestamp_end:
        _first_shares_balance_lt = ["$timestamp", 0]
        _operations_match.append({"$lte": ["$timestamp", timestamp_end]})
    # add block to match if set
    if block_ini and block_end:
        _first_shares_balance_lt = ["$block", block_ini]
        _operations_match.append({"$gte": ["$block", block_ini]})
        _operations_match.append({"$lte": ["$block", block_end]})
    elif block_ini:
        _first_shares_balance_lt = ["$block", block_ini]
        _operations_match.append({"$gte": ["$block", block_ini]})
    elif block_end:
        _first_shares_balance_lt = ["$block", 0]
        _operations_match.append({"$lte": ["$block", block_end]})

    query = [
        {"$sort": {"block": 1}},
        {
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "user_address": {"$first": "$user_address"},
                "hypervisor_address": {"$first": "$hypervisor_address"},
                "first_shares_balance": {
                    "$push": {
                        "$cond": {
                            "if": {
                                "$lt": (
                                    _first_shares_balance_lt
                                    if _first_shares_balance_lt
                                    else ["$block", 0]
                                )
                            },
                            "then": "$shares.balance",
                            "else": "$$REMOVE",
                        }
                    }
                },
                "operations": {
                    "$push": {
                        "$cond": {
                            "if": {
                                "$and": (
                                    _operations_match if _operations_match else [True]
                                )
                            },
                            "then": {
                                "id": "$id",
                                "block": "$block",
                                "timestamp": "$timestamp",
                                "user_address": "$user_address",
                                "hypervisor_address": "$hypervisor_address",
                                "shares": "$shares",
                                "tokens_flow": "$tokens_flow",
                                "prices": "$prices",
                                "topic": "$topic",
                                "transactionHash": "$transactionHash",
                                "logIndex": "$logIndex",
                                "customIndex": "$customIndex",
                            },
                            "else": "$$REMOVE",
                        }
                    }
                },
            }
        },
        {
            "$addFields": {
                "first_shares_balance": {
                    "$ifNull": [{"$last": "$first_shares_balance"}, "0"]
                },
                "last_shares_balance": {
                    "$ifNull": [
                        {"$last": "$operations.shares.balance"},
                        "$first_shares_balance",
                    ]
                },
            }
        },
    ]

    return query
