def query_hypervisor_operations_twa(
    hypervisor_address: str,
    block_ini: int | None = None,
    block_end: int | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
) -> list[dict]:
    """Return a list of sorted by block (not grouped) operations for a given hypervisor_address, with the option to filter by block or timestamp range.
        Operations with block<block_ini or timestamp<timestamp_ini should be considered as initial shares for the given user and not as operations inside the timeframe defined.

    Args:
        hypervisor_address (str):
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.
        timestamp_ini (int | None, optional): . Defaults to None.
        timestamp_end (int | None, optional): . Defaults to None.

    Returns:
        list[dict]: query to be used in mongo aggregation
    """

    # if no block or no timestamp is provided, return raise error
    if not block_ini and not timestamp_ini:
        raise Exception("block_ini or timestamp_ini must be provided")

    _match = {"hypervisor_address": hypervisor_address}

    # choose between blocks or timestamps
    if block_ini:
        _var = "$block"
        _value = block_ini
    elif timestamp_ini:
        _var = "$timestamp"
        _value = timestamp_ini
    if block_end:
        _match["block"] = {"$lte": block_end}
    elif timestamp_end:
        _match["timestamp"] = {"$lte": timestamp_end}

    return [
        {"$match": _match},
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": {"user": "$user_address"},
                "hypervisor_address": {"$first": "$hypervisor_address"},
                "end_block": {"$first": "$block"},
                "ini_block": {
                    "$max": {
                        "$cond": {
                            "if": {"$lt": [_var, _value]},
                            "then": _var,
                            "else": _value,
                        }
                    }
                },
                "ini_balance": {
                    "$push": {
                        "$cond": {
                            "if": {"$lt": [_var, _value]},
                            "then": "$shares.balance",
                            "else": 0,
                        }
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "user_operations",
                "let": {
                    "op_hype": "$hypervisor_address",
                    "op_user": "$_id.user",
                    "op_block_ini": "$ini_block",
                    "op_block_end": "$end_block",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$hypervisor_address", "$$op_hype"]},
                                    {"$eq": ["$user_address", "$$op_user"]},
                                    {"$gte": [_var, "$$op_block_ini"]},
                                    {"$lte": [_var, "$$op_block_end"]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "isReal": 0,
                            "logIndex": 0,
                            "customIndex": 0,
                            "id": 0,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "status",
                            "let": {
                                "op_hype": "$hypervisor_address",
                                "op_block": "$block",
                            },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                {"$eq": ["$address", "$$op_hype"]},
                                                {"$eq": ["$block", "$$op_block"]},
                                            ]
                                        }
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0,
                                        "hypervisor_decimals": "$decimals",
                                        "token0_decimals": "$pool.token0.decimals",
                                        "token1_decimals": "$pool.token1.decimals",
                                        "totalSupply": {"$toDecimal": "$totalSupply"},
                                        "underlying_value": {
                                            "token0": {
                                                "$sum": [
                                                    {
                                                        "$toDecimal": "$totalAmounts.total0"
                                                    },
                                                    {
                                                        "$toDecimal": "$fees_uncollected.lps_qtty_token0"
                                                    },
                                                ]
                                            },
                                            "token1": {
                                                "$sum": [
                                                    {
                                                        "$toDecimal": "$totalAmounts.total1"
                                                    },
                                                    {
                                                        "$toDecimal": "$fees_uncollected.lps_qtty_token1"
                                                    },
                                                ]
                                            },
                                        },
                                    }
                                },
                            ],
                            "as": "hypervisor_status",
                        }
                    },
                    {
                        "$addFields": {
                            "hypervisor_status": {"$first": "$hypervisor_status"},
                            "shares": {
                                "flow": {"$toDecimal": "$shares.flow"},
                                "balance": {"$toDecimal": "$shares.balance"},
                            },
                            "tokens_flow": {
                                "token0": {"$toDecimal": "$tokens_flow.token0"},
                                "token1": {"$toDecimal": "$tokens_flow.token1"},
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "shares.balance_percentage": {
                                "$divide": [
                                    "$shares.balance",
                                    "$hypervisor_status.totalSupply",
                                ]
                            },
                            "shares.balance_token0": {
                                "$cond": [
                                    {"$gt": ["$shares.balance", 0]},
                                    {
                                        "$multiply": [
                                            {
                                                "$divide": [
                                                    "$shares.balance",
                                                    "$hypervisor_status.totalSupply",
                                                ]
                                            },
                                            "$hypervisor_status.underlying_value.token0",
                                        ]
                                    },
                                    0,
                                ]
                            },
                            "shares.balance_token1": {
                                "$cond": [
                                    {"$gt": ["$shares.balance", 0]},
                                    {
                                        "$multiply": [
                                            {
                                                "$divide": [
                                                    "$shares.balance",
                                                    "$hypervisor_status.totalSupply",
                                                ]
                                            },
                                            "$hypervisor_status.underlying_value.token1",
                                        ]
                                    },
                                    0,
                                ]
                            },
                            "hypervisor_status.underlying_value.usd": {
                                "$sum": [
                                    {
                                        "$multiply": [
                                            "$prices.token0",
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.underlying_value.token0",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token0_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "$multiply": [
                                            "$prices.token1",
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.underlying_value.token1",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token1_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                            "tokens_flow.usd": {
                                "$toDouble": {
                                    "$sum": [
                                        {
                                            "$multiply": [
                                                "$prices.token0",
                                                {
                                                    "$divide": [
                                                        "$tokens_flow.token0",
                                                        {
                                                            "$pow": [
                                                                10,
                                                                "$hypervisor_status.token0_decimals",
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                        {
                                            "$multiply": [
                                                "$prices.token1",
                                                {
                                                    "$divide": [
                                                        "$tokens_flow.token1",
                                                        {
                                                            "$pow": [
                                                                10,
                                                                "$hypervisor_status.token1_decimals",
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                }
                            },
                            "prices.share": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {
                                                "$gt": [
                                                    "$hypervisor_status.underlying_value.token0",
                                                    0,
                                                ]
                                            },
                                            {
                                                "$gt": [
                                                    "$hypervisor_status.underlying_value.token1",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "$divide": [
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.totalSupply",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.hypervisor_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                            {
                                                "$sum": [
                                                    {
                                                        "$multiply": [
                                                            "$prices.token0",
                                                            {
                                                                "$divide": [
                                                                    "$hypervisor_status.underlying_value.token0",
                                                                    {
                                                                        "$pow": [
                                                                            10,
                                                                            "$hypervisor_status.token0_decimals",
                                                                        ]
                                                                    },
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    {
                                                        "$multiply": [
                                                            "$prices.token1",
                                                            {
                                                                "$divide": [
                                                                    "$hypervisor_status.underlying_value.token1",
                                                                    {
                                                                        "$pow": [
                                                                            10,
                                                                            "$hypervisor_status.token1_decimals",
                                                                        ]
                                                                    },
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    0,
                                ]
                            },
                        }
                    },
                    {"$sort": {"block": 1}},
                ],
                "as": "operations",
            }
        },
        {"$unwind": {"path": "$operations", "preserveNullAndEmptyArrays": True}},
        {"$sort": {"operations.block": 1}},
        {"$replaceRoot": {"newRoot": "$operations"}},
    ]
