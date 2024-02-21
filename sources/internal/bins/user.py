#
#


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
        collection_name="user_operations",
        aggregate=query_all_user_addresses(hypervisor_address=hypervisor_address),
    )


async def get_user_shares(
    user_address: str,
    chain: Chain,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """User shares at a specific time ( default is last known)."""
    return [
        global_database_helper().convert_d128_to_decimal(x)
        for x in await local_database_helper(network=chain).get_items_from_database(
            collection_name="user_operations",
            aggregate=query_user_shares_merkl(
                user_address=user_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
                block_ini=block_ini,
                block_end=block_end,
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
        {"$unwind": {"path": "$user_addresses", "preserveNullAndEmptyArrays": False}},
        {"$group": {"_id": "$user_addresses"}},
        {"$project": {"user_address": "$_id", "_id": 0}},
    ]

    if hypervisor_address:
        _query[0]["$match"] = {"hypervisor.address": hypervisor_address}

    return _query


def query_user_shares_merkl(
    user_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):
    """Query to get user shares from the database at a specific time
        ( user_operations collection )

    Args:
        user_address (str): user address to query
        timestamp_ini (int | None, optional): . Defaults to None.
        timestamp_end (int | None, optional): . Defaults to None.
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.
    """

    # build standard match
    _match = {
        "$and": [
            {
                "$or": [
                    {"sender": user_address},
                    {"to": user_address},
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

    _query = [
        {"$match": _match},
        {"$sort": {"blockNumber": 1}},
        {
            "$lookup": {
                "from": "status",
                "let": {
                    "op_address": "$hypervisor.address",
                    "op_block": "$blockNumber",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$address", "$$op_address"]},
                                    {"$eq": ["$block", "$$op_block"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                    {
                        "$project": {
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
                    {"$unset": ["_id"]},
                ],
                "as": "hype_status",
            }
        },
        {
            "$project": {
                "_id": 0,
                "block": "$blockNumber",
                "timestamp": "$timestamp",
                "topic": "$topic",
                "operation_shares": {"$toDecimal": "$shares"},
                "info": "$hypervisor",
                "user_shares": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$topic", "deposit"]},
                                        {
                                            "$and": [
                                                {"$eq": ["$topic", "transfer"]},
                                                {
                                                    "$eq": [
                                                        "$to",
                                                        user_address,
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                },
                                {"$toDecimal": {"$ifNull": ["$qtty", "$shares"]}},
                                {
                                    "$cond": [
                                        {
                                            "$or": [
                                                {"$eq": ["$topic", "withdraw"]},
                                                {
                                                    "$and": [
                                                        {"$eq": ["$topic", "transfer"]},
                                                        {
                                                            "$eq": [
                                                                "$sender",
                                                                user_address,
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                        {
                                            "$multiply": [
                                                {
                                                    "$toDecimal": {
                                                        "$ifNull": ["$qtty", "$shares"]
                                                    }
                                                },
                                                -1,
                                            ]
                                        },
                                        0,
                                    ]
                                },
                            ]
                        },
                        0,
                    ]
                },
                "total_shares": {"$first": "$hype_status.totalSupply"},
                "total_token0": {"$first": "$hype_status.underlying_qtty0"},
                "total_token1": {"$first": "$hype_status.underlying_qtty1"},
            }
        },
        {
            "$group": {
                "_id": {"hype": "$info.address"},
                "last_block": {"$last": "$block"},
                "last_timestamp": {"$last": "$timestamp"},
                "info": {"$first": "$info"},
                "user_shares": {"$sum": "$user_shares"},
                "operations": {
                    "$push": {
                        "block": "$block",
                        "timestamp": "$timestamp",
                        "topic": "$topic",
                        "shares": "$operation_shares",
                        "total_shares": "$total_shares",
                        "total_token0": "$total_token0",
                        "total_token1": "$total_token1",
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "status",
                "let": {"op_address": "$_id.hype"},
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
                            "block": "$block",
                            "timestamp": "$timestamp",
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
                    {"$unset": ["_id"]},
                ],
                "as": "last_hype_status",
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor": "$_id.hype",
                "timestamp": {"$first": "$last_hype_status.timestamp"},
                "block": {"$first": "$last_hype_status.block"},
                "user_shares": "$user_shares",
                "hypervisor_info": "$info",
                "total_shares": {"$first": "$last_hype_status.totalSupply"},
                "total_token0": {"$first": "$last_hype_status.underlying_qtty0"},
                "total_token1": {"$first": "$last_hype_status.underlying_qtty1"},
                "operations": "$operations",
            }
        },
    ]

    return _query
