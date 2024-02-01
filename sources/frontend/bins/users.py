import asyncio
from sources.common.general.enums import Chain
from sources.mongo.bins.apps.prices import get_current_prices, get_prices
from sources.mongo.bins.helpers import global_database_helper, local_database_helper
from sources.mongo.endpoint.routers import DEPLOYED


async def get_user_positions(user_address: str, chain: Chain) -> list[dict]:
    # 1) Get the user positions from operations
    return [
        global_database_helper().convert_d128_to_decimal(x)
        for x in await local_database_helper(network=chain).get_items_from_database(
            collection_name="operations",
            aggregate=query_user_positions(user_address=user_address),
        )
    ]

    # 2) Identfy staked positions ( current zero shares last deposit == known rewarder )


def query_user_positions(user_address: str) -> list[dict]:
    """

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
