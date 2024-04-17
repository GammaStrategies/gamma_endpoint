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

    _and = []
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
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "user_address": {"$first": "$user_address"},
                "start_block_number": {"$first": "$block"},
                "end_block_number": {"$last": "$block"},
                "receiptInfos": {
                    "$push": {
                        "$cond": [
                            {"$eq": ["$customIndex", 0]},
                            {
                                "transaction_hash": "$transactionHash",
                                "log_extract_infos": {
                                    "value_from_topic": False,
                                    "log_index": "$logIndex",
                                    "log_index_tx": -1,
                                    "value_index": 0,
                                },
                            },
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "$_id.hype",
                "hypervisor_address": {"$first": "$_id.hype"},
                "users_data": {
                    "$push": {
                        "user_address": "$user_address",
                        "start_block_number": "$start_block_number",
                        "end_block_number": "$end_block_number",
                        "receiptInfos": {"$setDifference": ["$receiptInfos", [None]]},
                    }
                },
            }
        },
    ]

    if _and:
        _query.insert(0, {"$match": {"$and": _and}})

    return _query
