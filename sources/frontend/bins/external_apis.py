# MultiFeeDistributor v2


import asyncio
import logging

from sources.common.database.common.collections_common import db_collections_common
from sources.common.general.enums import Chain, Protocol
from sources.common.xt_api.ramses import ramses_api_helper
from sources.mongo.bins.helpers import local_database_helper


async def get_ramsesLike_api_data(chain: Chain, protocol: Protocol) -> dict:

    # create result variable
    result = {}

    # get all pools
    try:
        ramses_api = ramses_api_helper(chain=chain)
        if not ramses_api:
            logging.getLogger(__name__).warning("Ramses API not available")
            return []

        # build query
        _query = [
            {"$match": {"dex": protocol.database_name}},
            {
                "$lookup": {
                    "from": "rewards_static",
                    "let": {"op_hype_address": "$address"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$hypervisor_address",
                                                "$$op_hype_address",
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "rewarder_address": 1,
                                "rewarder_type": 1,
                                "rewarder_registry": 1,
                                "rewardToken": 1,
                                "rewardToken_decimals": 1,
                                "rewardToken_symbol": 1,
                            }
                        },
                        {"$sort": {"block": 1}},
                    ],
                    "as": "rewards_static",
                }
            },
        ]

        # get all hypervisors and api data
        tmp, hypervisors_static_list = await asyncio.gather(
            ramses_api.data(),
            local_database_helper(network=chain).get_items_from_database(
                collection_name="static",
                aggregate=_query,
            ),
        )

        for hypervisor in hypervisors_static_list:
            _pool_address = hypervisor["pool"]["address"]
            _gauge_address = hypervisor["address"]

            # get apr data from API
            _ramapr_data = await ramses_api.get_apr(pool=_pool_address)
            if not _ramapr_data:
                continue

            for rewardToken, aprData in _ramapr_data.items():
                # search rewardToken in rewards_static
                _rewarder = next(
                    (
                        x
                        for x in hypervisor["rewards_static"]
                        if x["rewardToken"].lower() == rewardToken.lower()
                    ),
                    None,
                )
                if not _rewarder:
                    logging.getLogger(__name__).error(
                        f"rewardToken {rewardToken} not found in rewards_static"
                    )

                # add data to result
                if not _gauge_address in result:
                    result[_gauge_address] = {"hypervisors": {}}
                if not hypervisor["address"] in result[_gauge_address]["hypervisors"]:
                    result[_gauge_address]["hypervisors"][hypervisor["address"]] = {
                        "stakeTokenSymbol": hypervisor["symbol"],
                        # "stakedAmount": aprData["totalValueLockedUSD"],
                        "stakedAmountUSD": aprData["totalValueLockedUSD"],
                        "apr": 0,
                        "baseApr": 0,
                        "boostApr": 0,
                        "rewarders": {},
                    }

                # only add data if there is any reward apr
                if aprData["apr"]:
                    # add rewarder data
                    result[_gauge_address]["hypervisors"][hypervisor["address"]][
                        "rewarders"
                    ][rewardToken] = {
                        # "timestamp": aprData["timestamp"],
                        "rewardToken": rewardToken,
                        "rewardToken_price": aprData["price"],
                        "rewardTokenDecimals": aprData["rewardToken_decimals"],
                        "rewardTokenSymbol": aprData["rewardToken_symbol"],
                        "rewardPerSecond": aprData["rewardsPerSecond"],
                        "apr": aprData["apr"],
                        "baseApr": aprData["apr"],
                        "boostApr": 0,
                        "baseRewardPerSecond": aprData["rewardRate"],
                        "boostRewardPerSecond": 0,
                    }
                    # add to totals
                    result[_gauge_address]["hypervisors"][hypervisor["address"]][
                        "apr"
                    ] += aprData["apr"]
                    result[_gauge_address]["hypervisors"][hypervisor["address"]][
                        "baseApr"
                    ] += aprData["apr"]
                    result[_gauge_address]["hypervisors"][hypervisor["address"]][
                        "boostApr"
                    ] += 0

    except Exception as e:
        logging.getLogger(__name__).error(
            f"Error pulling and formating ramses like api data: {e}"
        )

    return result


async def get_xpoints_xtrade(
    include_contracts: bool = True, include_transfers: bool = False
) -> list:

    xtoken_address = "0x9d2e7411b91aff3e88e196a4d3c40420376fbcf9".lower()
    _project = {
        "user_address": "$_id",
        "balance": {"$toString": "$balance"},
        "isContract": 1,
    }
    if include_transfers:
        _project["items"] = 1

    _query = [
        {"$match": {"topic": {"$in": ["transfer", "mint"]}, "address": xtoken_address}},
        {"$unset": "_id"},
        {
            "$addFields": {
                "user_address": ["$from", "$to"],
                "value": {"$cond": [{"$gt": ["$value", None]}, "$value", "$amount"]},
            }
        },
        {"$unwind": {"path": "$user_address", "preserveNullAndEmptyArrays": False}},
        {"$match": {"user_address": {"$ne": None}}},
        {
            "$addFields": {
                "balance": {
                    "$cond": [
                        {"$eq": ["$user_address", "$to"]},
                        {"$toDecimal": "$value"},
                        {"$multiply": [{"$toDecimal": "$value"}, -1]},
                    ]
                }
            }
        },
        {"$sort": {"blockNumber": 1, "logIndex": 1}},
        {
            "$group": {
                "_id": "$user_address",
                "items": {"$push": "$$ROOT"},
                "balance": {"$sum": "$balance"},
            }
        },
        {
            "$addFields": {
                "isContract": {
                    "$cond": [
                        {"$eq": ["$_id", {"$first": "$items.from"}]},
                        {"$first": "$items.from_isContract"},
                        {"$first": "$items.to_isContract"},
                    ]
                }
            }
        },
        {"$sort": {"balance": -1}},
        {"$project": _project},
    ]

    if not include_contracts:
        _query.append({"$match": {"isContract": False}})

    return [
        db_collections_common.convert_d128_to_decimal(x)
        for x in await local_database_helper(
            network=Chain.XLAYER
        ).get_items_from_database(
            collection_name="token_operations",
            aggregate=_query,
        )
    ]
