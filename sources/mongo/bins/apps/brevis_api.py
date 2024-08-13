from sources.common.general.enums import Chain
from sources.mongo.bins.helpers import local_database_helper


async def build_brevisQueryRequest(
    network: Chain,
    user_address: str | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
) -> dict:
    """"""
    return await local_database_helper(network=network).query_items_from_database(
        collection_name="user_operations",
        query=query_user_operations_brevis_GammaQueryRequest(
            user_address=user_address,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            hypervisor_address=hypervisor_address,
        ),
    )


def query_user_operations_brevis_GammaQueryRequest(
    user_address: str | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
) -> list[dict]:

    # make sure we only get the topics we need
    _and = [
        {
            "topic": {
                "$in": ["transfer", "withdraw", "deposit"],
            },
        }
    ]
    if user_address:
        _and.append({"user_address": user_address})
    if block_ini and block_end:
        _and.append({"block": {"$gte": block_ini, "$lte": block_end}})
    elif block_ini:
        _and.append({"block": {"$gte": block_ini}})
    elif block_end:
        _and.append({"block": {"$lte": block_end}})

    if timestamp_ini and timestamp_end:
        _and.append({"timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end}})
    elif timestamp_ini:
        _and.append({"timestamp": {"$gte": timestamp_ini}})
    elif timestamp_end:
        _and.append({"timestamp": {"$lte": timestamp_end}})

    if hypervisor_address:
        _and.append({"hypervisor_address": hypervisor_address})

    _query = [
        {
            "$lookup": {
                "from": "transaction_receipts",
                "let": {
                    "op_txHash": "$transactionHash",
                    "op_topic": "$topic",
                    "op_shares": {"$toDecimal": "$shares.flow"},
                    "op_logIndex": "$logIndex",
                },
                "pipeline": [
                    {"$match": {"$expr": {"$and": [{"$eq": ["$id", "$$op_txHash"]}]}}},
                    {
                        "$project": {
                            "logIndex": "$$op_logIndex",
                            "value_index": {
                                "$cond": [
                                    {"$eq": ["$$op_topic", "withdraw"]},
                                    1,
                                    {
                                        "$cond": [
                                            {"$eq": ["$$op_topic", "deposit"]},
                                            2,
                                            # 3 is for the case of a transfer: 1 for the sender (negative share flow), 2 for the receiver ( positive flow)
                                            {
                                                "$cond": [
                                                    {"$gt": ["$$op_shares", 0]},
                                                    2,
                                                    1,
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                            "logIndex_tx": {
                                "$indexOfArray": ["$logs.logIndex", "$$op_logIndex"]
                            },
                        }
                    },
                ],
                "as": "transaction_info",
            }
        },
        {"$sort": {"block": 1, "logIndex": 1, "customIndex": 1}},
        {
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "user_address": {"$first": "$user_address"},
                "hypervisor_address": {"$first": "$hypervisor_address"},
                "start_block_number": {"$first": "$block"},
                "end_block_number": {"$last": "$block"},
                "receiptInfos": {
                    "$push": {
                        "$cond": [
                            {"$eq": ["$isReal", True]},
                            {
                                "block_number": "$block",
                                "transaction_hash": "$transactionHash",
                                "topic": "$topic",
                                "log_extract_infos": [
                                    {
                                        "value_from_topic": True,
                                        "log_index": "$logIndex",
                                        "log_index_tx": {
                                            "$first": "$transaction_info.logIndex_tx"
                                        },
                                        "value_index": {
                                            "$first": "$transaction_info.value_index"
                                        },
                                    },
                                    {
                                        "value_from_topic": False,
                                        "log_index": "$logIndex",
                                        "log_index_tx": {
                                            "$first": "$transaction_info.logIndex_tx"
                                        },
                                        "value_index": 0,
                                    },
                                ],
                            },
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "user_address": 1,
                "hypervisor_address": 1,
                "start_block_number": 1,
                "end_block_number": 1,
                "receiptInfos": {"$setDifference": ["$receiptInfos", [None]]},
            }
        },
    ]

    if _and:
        _query.insert(0, {"$match": {"$and": _and}})

    return _query
