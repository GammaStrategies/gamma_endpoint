import asyncio

from bson import Decimal128
from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import global_database_helper, local_database_helper


async def get_user_positions(user_address: str, chain: Chain) -> list[dict]:
    """User positions from the POV of the user operations collection.

    Args:
        user_address (str): user address to query
        chain (Chain): chain to query

    Returns:
        list[dict]: list of user positions

            hypervisor
            current_usd_value
            PnL
            block
            timestamp
            info
                address
                ...
            share_percent
            shares
            current_supply
            token0
            token1
            share_price
            operations
                ...


    """
    # 1) Get the user positions
    return [
        global_database_helper().convert_d128_to_decimal(x)
        for x in await local_database_helper(network=chain).get_items_from_database(
            collection_name="user_operations",
            aggregate=query_user_operations_current_info(
                chain=chain, user_address=user_address
            ),
        )
    ]


# QUERIES


def query_user_positions(
    user_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """User positions from the POV of the user operations collection.
        This will show all user addresses, including those hidden behind proxied deposits ( loke Camelot spNFT )

    Args:
        user_address (str): user address to query
        timestamp_ini (int | None, optional): initial timestamp to include operations from (including). Defaults to None.
        timestamp_end (int | None, optional): end timestamp. Defaults to None.
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.

    Returns:
        list[dict]:
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

    # add timestamp and block filters
    if timestamp_ini and timestamp_end:
        _match["$and"].append(
            {"timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end}}
        )
    elif timestamp_ini:
        _match["$and"].append({"timestamp": {"$gte": timestamp_ini}})
    elif timestamp_end:
        _match["$and"].append({"timestamp": {"$lte": timestamp_end}})
    #
    if block_ini and block_end:
        _match["$and"].append({"blockNumber": {"$gte": block_ini, "$lte": block_end}})
    elif block_ini:
        _match["$and"].append({"blockNumber": {"$gte": block_ini}})
    elif block_end:
        _match["$and"].append({"blockNumber": {"$lte": block_end}})

    # build query
    _query = [
        {"$match": _match},
        {"$sort": {"blockNumber": 1}},
        {
            "$lookup": {
                "from": "hypervisor_returns",
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
                                    {"$gte": ["$timeframe.ini.block", "$$op_block"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                    {
                        "$project": {
                            "token_prices": "$status.ini.prices",
                            "share_price": {
                                "$divide": [
                                    {
                                        "$sum": [
                                            {
                                                "$multiply": [
                                                    "$status.ini.prices.token0",
                                                    "$status.ini.underlying.qtty.token0",
                                                ]
                                            },
                                            {
                                                "$multiply": [
                                                    "$status.ini.prices.token1",
                                                    "$status.ini.underlying.qtty.token1",
                                                ]
                                            },
                                        ]
                                    },
                                    "$status.ini.supply",
                                ]
                            },
                        }
                    },
                    {"$unset": ["_id"]},
                ],
                "as": "hype_returns",
            }
        },
        {
            "$project": {
                "_id": 0,
                "block": "$blockNumber",
                "timestamp": "$timestamp",
                "topic": "$topic",
                "info": "$hypervisor",
                "shares": {
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
                                                                "$from",
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
                "token0": {
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
                                {"$toDecimal": {"$ifNull": ["$qtty_token0", 0]}},
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
                                                                "$from",
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
                                                        "$ifNull": ["$qtty_token0", 0]
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
                "token1": {
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
                                {"$toDecimal": {"$ifNull": ["$qtty_token1", 0]}},
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
                                                                "$from",
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
                                                        "$ifNull": ["$qtty_token1", 0]
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
                "prices": {
                    "share_price": {"$first": "$hype_returns.share_price"},
                    "token0": {"$first": "$hype_returns.token_prices.token0"},
                    "token1": {"$first": "$hype_returns.token_prices.token1"},
                },
            }
        },
        {
            "$group": {
                "_id": {"hype": "$info.address"},
                "last_block": {"$last": "$block"},
                "last_timestamp": {"$last": "$timestamp"},
                "info": {"$first": "$info"},
                "operations_shares": {"$sum": "$shares"},
                "operations_token0": {"$sum": "$token0"},
                "operations_token1": {"$sum": "$token1"},
                "operations": {
                    "$push": {
                        "block": "$block",
                        "timestamp": "$timestamp",
                        "topic": "$topic",
                        "shares": "$shares",
                        "token0": "$token0",
                        "token1": "$token1",
                        "share_price": "$prices.share_price",
                        "token0_price": "$prices.token0",
                        "token1_price": "$prices.token1",
                        "usd_value": {
                            "$multiply": [
                                {
                                    "$divide": [
                                        {"$multiply": [-1, "$shares"]},
                                        {"$pow": [10, "$info.decimals"]},
                                    ]
                                },
                                "$prices.share_price",
                            ]
                        },
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "hypervisor_returns",
                "let": {"op_address": "$_id.hype"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                    {"$sort": {"timeframe.end.block": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "share_price": {
                                "$divide": [
                                    {
                                        "$sum": [
                                            {
                                                "$multiply": [
                                                    "$status.end.prices.token0",
                                                    "$status.end.underlying.qtty.token0",
                                                ]
                                            },
                                            {
                                                "$multiply": [
                                                    "$status.end.prices.token1",
                                                    "$status.end.underlying.qtty.token1",
                                                ]
                                            },
                                        ]
                                    },
                                    "$status.end.supply",
                                ]
                            },
                            "supply": "$status.end.supply",
                            "token0_qtty": "$status.end.underlying.qtty.token0",
                            "token1_qtty": "$status.end.underlying.qtty.token1",
                        }
                    },
                ],
                "as": "last_hypervisor_returns",
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor": "$_id.hype",
                "current_usd_value": {
                    "$cond": [
                        {"$gt": ["$operations_shares", 0]},
                        {
                            "$multiply": [
                                {
                                    "$divide": [
                                        "$operations_shares",
                                        {"$pow": [10, "$info.decimals"]},
                                    ]
                                },
                                {"$first": "$last_hypervisor_returns.share_price"},
                            ]
                        },
                        Decimal128("0"),
                    ]
                },
                "operations_pnl": {"$sum": ["$operations.usd_value"]},
                "block": "$last_block",
                "timestamp": "$last_timestamp",
                "info": "$info",
                "shares": "$operations_shares",
                "current_supply": {
                    "$multiply": [
                        {"$first": "$last_hypervisor_returns.supply"},
                        {"$pow": [10, "$info.decimals"]},
                    ]
                },
                "total_token0": {
                    "$multiply": [
                        {"$first": "$last_hypervisor_returns.token0_qtty"},
                        {"$pow": [10, "$info.token0_decimals"]},
                    ]
                },
                "total_token1": {
                    "$multiply": [
                        {"$first": "$last_hypervisor_returns.token1_qtty"},
                        {"$pow": [10, "$info.token1_decimals"]},
                    ]
                },
                "share_price": {"$first": "$last_hypervisor_returns.share_price"},
                "operations": "$operations",
            }
        },
        {
            "$project": {
                "hypervisor": "$hypervisor",
                "current_usd_value": "$current_usd_value",
                "PnL": {"$sum": ["$operations_pnl", "$current_usd_value"]},
                "block": "$block",
                "timestamp": "$timestamp",
                "info": "$info",
                "share_percent": {
                    "$cond": [
                        {"$gt": ["$shares", 0]},
                        {"$divide": ["$shares", "$current_supply"]},
                        Decimal128("0"),
                    ]
                },
                "shares": "$shares",
                "current_supply": "$current_supply",
                "token0": {
                    "$cond": [
                        {"$gt": ["$shares", 0]},
                        {
                            "$multiply": [
                                "$total_token0",
                                {"$divide": ["$shares", "$current_supply"]},
                            ]
                        },
                        Decimal128("0"),
                    ]
                },
                "token1": {
                    "$cond": [
                        {"$gt": ["$shares", 0]},
                        {
                            "$multiply": [
                                "$total_token1",
                                {"$divide": ["$shares", "$current_supply"]},
                            ]
                        },
                        Decimal128("0"),
                    ]
                },
                "share_price": "$share_price",
                "operations": "$operations",
            }
        },
    ]

    return _query


def query_user_positions_from_global_operations(user_address: str) -> list[dict]:
    """User positions from the POV of the operations collection.
        This will not show user addresses hidden behind proxied deposits ( loke Camelot spNFT )

    Returns:
     the database will return a list of :
        {
        hypervisor: "0x0000.."
        last_block: 0000
        last_timestamp: 0000

        info {
            hypervisor_symbol: "wstETH-ETH3"
            token0_address: "0x5979d7b546e38e414f7e9822514be443a4800529"
            token1_address:"0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
            decimals_hype:18
            decimals_token0:18
            decimals_token1:18
        }

        last_shares:0
        last_token0: 002312312212121
        last_token1: -122222254646
        last_share_price: 123.9947843234

        operations:[
            {
                block:66896174
                timestamp: 1677993616
                topic:"deposit"
                shares: 443268479662442397
                token0: 2696738539132030705
                token1: 785083692322936013
                share_price: 13385.15685806833258923989827337968
                token0_price: 1743.6310375106418
                token1_price: 1568.1144897579422
        }
        ...
        ]
        }

    """
    _query = [
        {
            "$match": {
                "$and": [
                    {
                        "$or": [
                            {"src": user_address},
                            {"dst": user_address},
                            {"sender": user_address},
                            {"to": user_address},
                        ]
                    },
                    {"src": {"$ne": "0x0000000000000000000000000000000000000000"}},
                    {"dst": {"$ne": "0x0000000000000000000000000000000000000000"}},
                ]
            }
        },
        {"$sort": {"blockNumber": 1}},
        {
            "$lookup": {
                "from": "static",
                "let": {"op_address": "$address"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "address": "$address",
                            "symbol": "$symbol",
                            "pool": {
                                "address": "$pool.address",
                                "token0": "$pool.token0.address",
                                "token1": "$pool.token1.address",
                                "dex": "$pool.dex",
                            },
                            "dex": "$dex",
                        }
                    },
                    {"$unset": ["_id"]},
                ],
                "as": "static",
            }
        },
        {
            "$lookup": {
                "from": "hypervisor_returns",
                "let": {"op_address": "$address", "op_block": "$blockNumber"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$address", "$$op_address"]},
                                    {"$gte": ["$timeframe.ini.block", "$$op_block"]},
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                    {
                        "$project": {
                            "token_prices": "$status.ini.prices",
                            "share_price": {
                                "$divide": [
                                    {
                                        "$sum": [
                                            {
                                                "$multiply": [
                                                    "$status.ini.prices.token0",
                                                    "$status.ini.underlying.qtty.token0",
                                                ]
                                            },
                                            {
                                                "$multiply": [
                                                    "$status.ini.prices.token1",
                                                    "$status.ini.underlying.qtty.token1",
                                                ]
                                            },
                                        ]
                                    },
                                    "$status.ini.supply",
                                ]
                            },
                        }
                    },
                    {"$unset": ["_id"]},
                ],
                "as": "hype_returns",
            }
        },
        {
            "$project": {
                "_id": 0,
                "block": "$blockNumber",
                "timestamp": "$timestamp",
                "hypervisor": "$address",
                "hypervisor_symbol": {"$first": "$static.symbol"},
                "token0_address": {"$first": "$static.pool.token0"},
                "token1_address": {"$first": "$static.pool.token1"},
                "decimals_contract": "$decimals_contract",
                "decimals_token0": "$decimals_token0",
                "decimals_token1": "$decimals_token1",
                "topic": "$topic",
                "shares": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {
                                    "$or": [
                                        {"$eq": ["$topic", "deposit"]},
                                        {"$eq": ["$dst", user_address]},
                                    ]
                                },
                                {"$toDecimal": {"$ifNull": ["$qtty", "$shares"]}},
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
                            ]
                        },
                        0,
                    ]
                },
                "token0": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {"$eq": ["$topic", "deposit"]},
                                {"$toDecimal": "$qtty_token0"},
                                {"$multiply": [{"$toDecimal": "$qtty_token0"}, -1]},
                            ]
                        },
                        0,
                    ]
                },
                "token1": {
                    "$ifNull": [
                        {
                            "$cond": [
                                {"$eq": ["$topic", "deposit"]},
                                {"$toDecimal": "$qtty_token1"},
                                {"$multiply": [{"$toDecimal": "$qtty_token1"}, -1]},
                            ]
                        },
                        0,
                    ]
                },
                "prices": {
                    "share_price": {"$first": "$hype_returns.share_price"},
                    "token0": {"$first": "$hype_returns.token_prices.token0"},
                    "token1": {"$first": "$hype_returns.token_prices.token1"},
                },
            }
        },
        {
            "$group": {
                "_id": {"hype": "$hypervisor"},
                "last_block": {"$last": "$block"},
                "last_timestamp": {"$last": "$timestamp"},
                "info": {
                    "$first": {
                        "hypervisor_symbol": "$hypervisor_symbol",
                        "token0_address": "$token0_address",
                        "token1_address": "$token1_address",
                        "decimals_hype": "$decimals_contract",
                        "decimals_token0": "$decimals_token0",
                        "decimals_token1": "$decimals_token1",
                    }
                },
                "last_shares": {"$sum": "$shares"},
                "last_token0": {"$sum": "$token0"},
                "last_token1": {"$sum": "$token1"},
                "operations": {
                    "$push": {
                        "block": "$block",
                        "timestamp": "$timestamp",
                        "topic": "$topic",
                        "shares": "$shares",
                        "token0": "$token0",
                        "token1": "$token1",
                        "share_price": "$prices.share_price",
                        "token0_price": "$prices.token0",
                        "token1_price": "$prices.token1",
                        "usd_value": {
                            "$multiply": [
                                {
                                    "$divide": [
                                        {"$multiply": [-1, "$shares"]},
                                        {"$pow": [10, "$decimals_contract"]},
                                    ]
                                },
                                "$prices.share_price",
                            ]
                        },
                    }
                },
                "price_id_token0": {
                    "$push": {
                        "$concat": [
                            "arbitrum_",
                            {"$toString": "$block"},
                            "_",
                            "$token0_address",
                        ]
                    }
                },
                "price_id_token1": {
                    "$push": {
                        "$concat": [
                            "arbitrum_",
                            {"$toString": "$block"},
                            "_",
                            "$token1_address",
                        ]
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "hypervisor_returns",
                "let": {"op_address": "$_id.hype"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                    {"$sort": {"block": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "share_price": {
                                "$divide": [
                                    {
                                        "$sum": [
                                            {
                                                "$multiply": [
                                                    "$status.end.prices.token0",
                                                    "$status.end.underlying.qtty.token0",
                                                ]
                                            },
                                            {
                                                "$multiply": [
                                                    "$status.end.prices.token1",
                                                    "$status.end.underlying.qtty.token1",
                                                ]
                                            },
                                        ]
                                    },
                                    "$status.end.supply",
                                ]
                            },
                        }
                    },
                ],
                "as": "current_share_price",
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor": "$_id.hype",
                "current_usd_value": {
                    "$multiply": [
                        "$last_shares",
                        {"$first": "$current_share_price.share_price"},
                    ]
                },
                "PnL": {"$sum": ["$operations.usd_value"]},
                "last_block": "$last_block",
                "last_timestamp": "$last_timestamp",
                "info": "$info",
                "last_shares": "$last_shares",
                "last_token0": "$last_token0",
                "last_token1": "$last_token1",
                "last_share_price": {"$first": "$current_share_price.share_price"},
                "operations": "$operations",
            }
        },
    ]
    return _query


def query_user_operations_shares_without_values_fast(user_address: str) -> list[dict]:
    """Query to be used in the user_operations collection to get the shares of a user without the values.
        The returned values have to be converted to decimal by using the tokens decimals ( not returned in this query )

    Args:
        user_address (str): user address to query

    Returns:
        list[dict]: query
    """
    _query = [
        {"$match": {"user_address": user_address}},
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "current_block": {"$last": "$block"},
                "current_share_balance": {"$last": {"$toDecimal": "$shares.balance"}},
                "tokens_deposited0": {"$sum": {"$toDecimal": "$tokens_flow.token0"}},
                "tokens_deposited1": {"$sum": {"$toDecimal": "$tokens_flow.token0"}},
            }
        },
    ]

    return _query


def query_user_operations_current_info(chain: Chain, user_address: str) -> list[dict]:
    """Query to be used in the user_operations collection to get the detailed position of a user at the
    latest block available in the database.
    It uses latest hypervisor returns to get latest hype prices and underlying quantities.
    It also aggregates all the user_opertions tokens flow to get the qtty of tokens deposited. Be aware that qtty deposited tokens are signed
    -->  they are positive when the user withdraws more tokens than already deposited.


    Args:
        chain (Chain): chain to query ( used to add chain and chain id to the results )
        user_address (str): user address to query

    Returns:
        list[dict]: query
    """
    _query = [
        {"$match": {"user_address": user_address}},
        {"$sort": {"block": -1}},
        {
            "$addFields": {
                "tokens_flow.usd_token0": {
                    "$sum": [
                        {
                            "$multiply": [
                                {"$toDecimal": "$tokens_flow.token0"},
                                "$prices.token0",
                            ]
                        }
                    ]
                },
                "tokens_flow.usd_token1": {
                    "$sum": [
                        {
                            "$multiply": [
                                {"$toDecimal": "$tokens_flow.token1"},
                                "$prices.token1",
                            ]
                        }
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "end_block": {"$first": "$block"},
                "ini_block": {"$last": "$block"},
                "ini_balance": {"$push": "$shares.balance"},
                "current_balance": {"$first": {"$toDecimal": "$shares.balance"}},
                "cost_token0": {"$sum": {"$toDecimal": "$tokens_flow.token0"}},
                "cost_token1": {"$sum": {"$toDecimal": "$tokens_flow.token1"}},
                "cost_usd_token0": {"$sum": {"$toDecimal": "$tokens_flow.usd_token0"}},
                "cost_usd_token1": {"$sum": {"$toDecimal": "$tokens_flow.usd_token1"}},
            }
        },
        {
            "$lookup": {
                "from": "static",
                "let": {"op_hype": "$_id.hype"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$address", "$$op_hype"]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "symbol": "$symbol",
                            "decimals": "$decimals",
                            "pool": "$pool",
                        }
                    },
                ],
                "as": "hypervisor_static",
            }
        },
        {
            "$unwind": {
                "path": "$hypervisor_static",
                "preserveNullAndEmptyArrays": False,
            }
        },
        {
            "$lookup": {
                "from": "latest_hypervisor_returns",
                "let": {
                    "op_hype": "$_id.hype",
                    "op_user": "$_id.user",
                    "op_hype_decimals": "$hypervisor_static.decimals",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$and": [{"$eq": ["$address", "$$op_hype"]}]}
                        }
                    },
                    {"$sort": {"timestmap.ini.block": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "block": "$timeframe.end.block",
                            "prices": "$status.end.prices",
                            "underlying_qtty": "$status.end.underlying",
                            "supply": "$status.end.supply",
                        }
                    },
                ],
                "as": "hype_returns",
            }
        },
        {
            "$project": {
                "_id": 0,
                "user": "$_id.user",
                "hypervisor": "$_id.hype",
                "current_balance": {
                    "$divide": [
                        "$current_balance",
                        {"$pow": [10, "$hypervisor_static.decimals"]},
                    ]
                },
                "current_share": {
                    "$divide": [
                        {
                            "$divide": [
                                "$current_balance",
                                {"$pow": [10, "$hypervisor_static.decimals"]},
                            ]
                        },
                        {"$last": "$hype_returns.supply"},
                    ]
                },
                "cost": {
                    "token0": {
                        "$divide": [
                            "$cost_token0",
                            {"$pow": [10, "$hypervisor_static.pool.token0.decimals"]},
                        ]
                    },
                    "token1": {
                        "$divide": [
                            "$cost_token1",
                            {"$pow": [10, "$hypervisor_static.pool.token1.decimals"]},
                        ]
                    },
                    "usd": {
                        "$sum": [
                            {
                                "$divide": [
                                    "$cost_usd_token0",
                                    {"$pow": [10, "$hypervisor_static.decimals"]},
                                ]
                            },
                            {
                                "$divide": [
                                    "$cost_usd_token1",
                                    {"$pow": [10, "$hypervisor_static.decimals"]},
                                ]
                            },
                        ]
                    },
                },
                "hype_returns": {"$first": "$hype_returns"},
            }
        },
        {
            "$project": {
                "chain": chain.database_name,
                "chain_id": {"$toInt":chain.id},
                "user": 1,
                "hypervisor": 1,
                "balance": {
                    "shares_qtty": "$current_balance",
                    "shares_percent": "$current_share",
                    "token0": {
                        "$multiply": [
                            "$hype_returns.underlying_qtty.qtty.token0",
                            "$current_share",
                        ]
                    },
                    "token1": {
                        "$multiply": [
                            "$hype_returns.underlying_qtty.qtty.token1",
                            "$current_share",
                        ]
                    },
                    "usd": {
                        "$sum": [
                            {
                                "$multiply": [
                                    "$hype_returns.underlying_qtty.qtty.token0",
                                    "$current_share",
                                    "$hype_returns.prices.token0",
                                ]
                            },
                            {
                                "$multiply": [
                                    "$hype_returns.underlying_qtty.qtty.token0",
                                    "$current_share",
                                    "$hype_returns.prices.token1",
                                ]
                            },
                        ]
                    },
                },
                "deposited": "$cost",
            }
        },
    ]

    return _query
