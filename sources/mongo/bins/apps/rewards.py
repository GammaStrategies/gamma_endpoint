import asyncio
from datetime import datetime, timezone
import logging
import time
from sources.common.database.collection_endpoint import database_global
from sources.common.general.enums import Chain, Protocol
from sources.common.xt_api.ramses import ramses_api_helper
from sources.internal.bins.user import query_user_shares_from_user_operations
from ..helpers import local_database_helper


# MultiFeeDistributor


async def latest_multifeeDistributor(network: Chain, protocol: Protocol):
    items = await local_database_helper(
        network=network
    ).get_latest_multifeedistribution(dex=protocol.database_name)

    result = {}
    for item in items:
        # add mfd address as root result
        if item["address"] not in result:
            result[item["address"]] = {"hypervisors": {}}

        # prepare vars
        stakedAmount = int(item.get("hypervisor_staked", 0)) / (
            10 ** item.get("hypervisor_static", {}).get("decimals", 0)
        )

        stakedAmountUSD = stakedAmount * item.get("hypervisor_share_price_usd", 0)

        # add hypervisor address in result
        if not item["hypervisor_address"] in result[item["address"]]["hypervisors"]:
            result[item["address"]]["hypervisors"][item["hypervisor_address"]] = {
                "stakeTokenSymbol": item["hypervisor_static"]["symbol"],
                "stakedAmount": stakedAmount,  # last staked amount
                "stakedAmountUSD": stakedAmountUSD,  # last staked amount usd
                "apr": 0,
                "baseApr": 0,
                "boostApr": 0,
                "rewarders": {},
            }

        # seconds elapsed since last update
        seconds_elapsed = item["seconds_sinceLastUpdateTime"]
        # rewards since last update
        baseRewards = float(item.get("baseRewards_sinceLastUpdateTime", 0))
        boostRewards = float(item.get("boostedRewards_sinceLastUpdateTime", 0))
        baseRewardPerSecond = baseRewards / seconds_elapsed
        boostRewardPerSecond = boostRewards / seconds_elapsed
        if baseRewardPerSecond.is_integer():
            baseRewardPerSecond = int(baseRewardPerSecond)
        if boostRewardPerSecond.is_integer():
            boostRewardPerSecond = int(boostRewardPerSecond)

        # calculate apr
        baseApr = item.get("apr_baseRewards", 0)
        boostApr = item.get("apr_boostedRewards", 0)

        # add data to rewarders
        if (
            not item["rewardToken"]
            in result[item["address"]]["hypervisors"][item["hypervisor_address"]][
                "rewarders"
            ]
        ):
            result[item["address"]]["hypervisors"][item["hypervisor_address"]][
                "rewarders"
            ][item["rewardToken"]] = {
                "timestamp": item["timestamp"],
                "rewardToken": item["rewardToken"],
                "rewardTokenDecimals": item["rewardToken_decimals"],
                "rewardTokenSymbol": item.get("rewardToken_symbol", None)
                or item["last_updated_data"].get("rewardToken_symbol", ""),
                "rewardPerSecond": baseRewardPerSecond + boostRewardPerSecond,
                "apr": baseApr + boostApr,
                "baseApr": baseApr,
                "boostApr": boostApr,
                "baseRewardPerSecond": baseRewardPerSecond,
                "boostRewardPerSecond": boostRewardPerSecond,
                "baseRewardsSinceLastUpdateTime": baseRewards,
                "boostRewardsSinceLastUpdateTime": boostRewards,
                "seconds_sinceLastUpdateTime": item["seconds_sinceLastUpdateTime"],
            }
        else:
            logging.getLogger(__name__).error(
                f"rewardToken {item['rewardToken']} already in rewarders"
            )

        # add to totals
        result[item["address"]]["hypervisors"][item["hypervisor_address"]]["apr"] += (
            baseApr + boostApr
        )
        result[item["address"]]["hypervisors"][item["hypervisor_address"]][
            "baseApr"
        ] += baseApr
        result[item["address"]]["hypervisors"][item["hypervisor_address"]][
            "boostApr"
        ] += boostApr

    # return result
    return await rewrite_mfd_with_api(result, network, protocol)
    # return await rewrite_mfd_with_custom(result, network)


async def rewrite_mfd_with_api(data: dict, chain: Chain, protocol: Protocol) -> dict:

    # gamma liquidity in range vs ramses_endpoint["liquidity"] * ramses_endpont calculated apr emission rate

    try:
        ramses_api = ramses_api_helper(chain=chain)
        if not ramses_api:
            logging.getLogger(__name__).warning("Ramses API not available")
            return data

        tmp, hypervisors_static, inrange_liquidity = await asyncio.gather(
            ramses_api.data(),
            local_database_helper(network=chain).get_items_from_database(
                collection_name="static",
                find={},
                projection={"_id": 0, "address": 1, "symbol": 1, "pool": 1},
            ),
            retrieve_liquidity_in_range(chain=chain, protocol=protocol),
        )
        hypervisors_static = {x["address"]: x for x in hypervisors_static}
        inrange_liquidity = {x["hypervisor_address"]: x for x in inrange_liquidity}

        for mfd_address, item in data.items():
            for hype_address, hype_data in item["hypervisors"].items():
                # reset totals
                hype_data["apr"] = 0
                hype_data["baseApr"] = 0
                hype_data["boostApr"] = 0

                for rewarder_address, rewarder_data in hype_data["rewarders"].items():
                    try:
                        # get ramses api data
                        tmp_data = await ramses_api.get_pool_apr(
                            pool=hypervisors_static[hype_address]["pool"]["address"],
                            token=rewarder_data["rewardToken"],
                        )
                    except Exception as e:
                        logging.getLogger(__name__).error(
                            f"Error getting ramses api data for pol {hypervisors_static[hype_address]['pool']['address']}: {e}"
                        )
                        continue

                    # calc gamma apr using liquidity inrange
                    if hype_address in inrange_liquidity:
                        # get the percentage of liquidity gamma has in range ( vs the pools total liquidity in range)
                        gamma_liquidity_inrange_percentage = (
                            (
                                inrange_liquidity[hype_address]["liquidity_inRange"]
                                / tmp_data["liquidity"]
                            )
                            if tmp_data["liquidity"] > 0
                            else 0
                        )
                        # get ramses apr emission rate
                        rewarder_data["rewardPerSecond"] = (
                            tmp_data["rewardRate"] * gamma_liquidity_inrange_percentage
                        )

                        rewarder_data["apr"] = (
                            (
                                (
                                    tmp_data["usdPerSecond"]
                                    * gamma_liquidity_inrange_percentage
                                    * 60
                                    * 60
                                    * 24
                                    * 365
                                )
                                / hype_data["stakedAmountUSD"]
                            )
                            if hype_data["stakedAmountUSD"] > 0
                            else 0
                        )
                        rewarder_data["baseApr"] = rewarder_data["apr"]
                        rewarder_data["boostApr"] = 0
                        rewarder_data["baseRewardPerSecond"] = rewarder_data[
                            "rewardPerSecond"
                        ]
                        rewarder_data["boostRewardPerSecond"] = 0
                        #### modify totals
                        ####

                        # hype_data["stakedAmount"] =
                        hype_data["apr"] += rewarder_data["apr"]
                        hype_data["baseApr"] += rewarder_data["baseApr"]
                        hype_data["boostApr"] += 0
                        hype_data["liquidity_inrange_gamma"] = inrange_liquidity[
                            hype_address
                        ]["liquidity_inRange"]
                        hype_data["liquidity_inrange_pool"] = tmp_data["liquidity"]

                    else:
                        logging.getLogger(__name__).error(
                            f" Hype {hype_address} not found in inrange liquidity data. Using EMISSION APR !!"
                        )
                        #### modify rewarder data
                        ####
                        rewarder_data["rewardPerSecond"] = tmp_data["rewardRate"]
                        rewarder_data["apr"] = tmp_data["apr"]
                        rewarder_data["baseApr"] = tmp_data["apr"]
                        rewarder_data["boostApr"] = 0
                        rewarder_data["baseRewardPerSecond"] = tmp_data["rewardRate"]
                        rewarder_data["boostRewardPerSecond"] = 0

                        #### modify totals
                        ####
                        hype_data["stakedAmountUSD"] = tmp_data["totalValueLockedUSD"]
                        # hype_data["stakedAmount"] =
                        hype_data["apr"] += tmp_data["apr"]
                        hype_data["baseApr"] += tmp_data["apr"]
                        hype_data["boostApr"] += 0

    except Exception as e:
        logging.getLogger(__name__).exception(f"Error rewriting mfd with api: {e}")

    return data


async def rewrite_mfd_with_custom(data: dict, chain: Chain):

    secons_back = 86400 * 2  # 2 days
    try:
        rewards_apr = {
            x["hypervisor_address"]: database_global.convert_decimal_to_float(
                database_global.convert_d128_to_decimal(x)
            )
            for x in await retrieve_rewards_from_hypervisor_returns(
                chain=chain,
                # hypervisor_addresses=list(data.keys()),
                timestamp_ini=datetime.now(tz=timezone.utc).timestamp() - secons_back,
            )
        }
        if not rewards_apr:
            logging.getLogger(__name__).warning(
                f"No rewards apr from hype returns available for {chain.fantasy_name}"
            )
            return data

        for mfd_address, item in data.items():
            for hype_address, hype_data in item["hypervisors"].items():

                if hype_address not in rewards_apr:
                    logging.getLogger(__name__).warning(
                        f"No rewards apr from hype returns available for {hype_address} in {chain.fantasy_name}"
                    )
                    continue

                # reset totals
                hype_data["apr"] = rewards_apr[hype_address]["rewards_APR"]
                hype_data["baseApr"] = rewards_apr[hype_address]["rewards_APR"]
                hype_data["boostApr"] = 0

                # for rewarder_address, rewarder_data in hype_data["rewarders"].items():

                # #### modify rewarder data
                # ####
                # rewarder_data["rewardPerSecond"] = tmp_data["rewardRate"]
                # rewarder_data["apr"] = tmp_data["apr"]
                # rewarder_data["baseApr"] = tmp_data["apr"]
                # rewarder_data["boostApr"] = 0
                # rewarder_data["baseRewardPerSecond"] = tmp_data["rewardRate"]
                # rewarder_data["boostRewardPerSecond"] = 0

                # #### modify totals
                # ####
                # hype_data["stakedAmountUSD"] = tmp_data["totalValueLockedUSD"]
                # # hype_data["stakedAmount"] =
                # hype_data["apr"] += tmp_data["apr"]
                # hype_data["baseApr"] += tmp_data["apr"]
                # hype_data["boostApr"] += 0

    except Exception as e:
        logging.getLogger(__name__).error(f"Error rewriting mfd with custom: {e}")

    return data


# Retrieve rewards from hypervisor returns


async def retrieve_rewards_from_hypervisor_returns(
    chain: Chain,
    hypervisor_addresses: list[str] | None = None,
    timestamp_ini: int | None = None,
) -> list[dict]:
    """Calculate rewards APR for hypervisors in a particular chain using the hypervisor returns collection:
    - rewards_APR = the sum of all rewards in the defined period / number of seconds of real data in the period (using the hypervisors periods) * 3600 * 24 * 365

    Args:
        chain (Chain):
        hypervisor_addresses (list[str] | None, optional): list of addresses to return. Defaults to All.
        timestamp_ini (int | None, optional): starting data point. Defaults to All.

    Returns:
        list[dict]: { "hypervisor_address": hypervisor address, "rewards_APR": 0.0  }

    """

    # 1) calculate rewards APR for all hypervisors in a particular chain using the hypervisor returns collection:
    #    - rewards_APR = the sum of all rewards in the defined period / number of seconds of real data in the period (using the hypervisors periods) * 3600 * 24 * 365
    _match = {}
    if hypervisor_addresses:
        _match["address"] = {"$in": hypervisor_addresses}
    if timestamp_ini:
        _match["timeframe.ini.timestamp"] = {"$gte": timestamp_ini}
    _query = [
        {
            "$group": {
                "_id": "$address",
                # we dont need the items in the result
                # "items": {"$push": "$$ROOT"},
                "rewards_period": {"$sum": "$rewards.period_yield"},
                # "fees_period": {"$sum": "$fees.period_yield"},
                # "fees_period_token0": {"$sum": "$fees.qtty.token0"},
                # "fees_period_token1": {"$sum": "$fees.qtty.token1"},
                "timeframe_in_secs": {"$sum": "$timeframe.seconds"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor_address": "$_id",
                "rewards_APR": {
                    "$cond": [
                        {"$gt": ["$timeframe_in_secs", 0]},
                        {
                            "$multiply": [
                                {"$divide": ["$rewards_period", "$timeframe_in_secs"]},
                                3600 * 24 * 365,
                            ]
                        },
                        0,
                    ]
                },
                # "fees_APR": {
                #     "$cond": [
                #         {"$gt": ["$timeframe_in_secs", 0]},
                #         {
                #             "$multiply": [
                #                 {"$divide": ["$fees_period", "$timeframe_in_secs"]},
                #                 3600 * 24 * 365,
                #             ]
                #         },
                #         0,
                #     ]
                # },
            }
        },
    ]
    if _match:
        _query.insert(0, {"$match": _match})

    # get
    return await local_database_helper(network=chain).get_items_from_database(
        collection_name="hypervisor_returns", aggregate=_query
    )


async def retrieve_liquidity_in_range(
    chain: Chain,
    hypervisor_addresses: list[str] | None = None,
    protocol: Protocol | None = None,
    timestamp_end: int | None = None,
    block_end: int | None = None,
) -> list[dict]:
    """Calculate liquidity in range for hypervisors in a particular chain using the hypervisor status collection:

    Args:
        chain (Chain):
        hypervisor_addresses (list[str] | None, optional): . Defaults to All.
        protocol (Protocol | None, optional): . Defaults to All.
        timestamp_end (int | None, optional): . Defaults to last known in the db.
        block_end (int | None, optional): . Defaults to last known in the db.

    Returns:
        list[dict]: { hypervisor_address, address, timestamp, block and liquidity_inRange (as float) }
    """

    _match = {}
    if hypervisor_addresses:
        _match["address"] = {"$in": hypervisor_addresses}
    if protocol:
        _match["dex"] = protocol.database_name
    if timestamp_end:
        _match["timestamp"] = {"$lte": timestamp_end}
    if block_end:
        _match["block"] = {"$lte": block_end}
    _query = [
        {"$sort": {"block": 1}},
        {
            "$group": {
                "_id": "$address",
                "last_item": {
                    "$last": {
                        "timestamp": "$timestamp",
                        "block": "$block",
                        "currentTick": {"$toDouble": "$currentTick"},
                        "baseUpper": {"$toDouble": "$baseUpper"},
                        "baseLower": {"$toDouble": "$baseLower"},
                        "limitUpper": {"$toDouble": "$limitUpper"},
                        "limitLower": {"$toDouble": "$limitLower"},
                        "baseLiquidityInRange": {
                            "$toDouble": "$basePosition.liquidity"
                        },
                        "limitLiquidityInRange": {
                            "$toDouble": "$limitPosition.liquidity"
                        },
                    }
                },
            }
        },
        {
            "$addFields": {
                "baseLiquidityInRange": {
                    "$cond": [
                        {
                            "$and": [
                                {
                                    "$gte": [
                                        "$last_item.baseUpper",
                                        "$last_item.currentTick",
                                    ]
                                },
                                {
                                    "$lte": [
                                        "$last_itemebaseLower",
                                        "$last_item.currentTick",
                                    ]
                                },
                            ]
                        },
                        "$last_item.baseLiquidityInRange",
                        0,
                    ]
                },
                "limitLiquidityInRange": {
                    "$cond": [
                        {
                            "$and": [
                                {
                                    "$gte": [
                                        "$last_item.limitUpper",
                                        "$last_item.currentTick",
                                    ]
                                },
                                {
                                    "$lte": [
                                        "$last_item.limitLower",
                                        "$last_item.currentTick",
                                    ]
                                },
                            ]
                        },
                        "$last_item.limitLiquidityInRange",
                        0,
                    ]
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "hypervisor_address": "$_id",
                "timestamp": "$last_item.timestamp",
                "block": "$last_item.block",
                "liquidity_inRange": {
                    "$sum": ["$baseLiquidityInRange", "$limitLiquidityInRange"]
                },
            }
        },
    ]

    if _match:
        _query.insert(0, {"$match": _match})

    return await local_database_helper(network=chain).get_items_from_database(
        collection_name="status", aggregate=_query
    )


# Gamma Merkl Rewards


# TODO: in devtest
async def calculate_gamma_merkle_rewards(
    chain: Chain,
    user_address: str | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    hypervisor_address: str | None = None,
):

    _startime = time.time()

    # { <user_address>: {<hypervisor_address>:{...data..} } }
    users_result = {}
    hypervisors_totals = {}
    for userHype in await local_database_helper(network=chain).get_items_from_database(
        collection_name="user_operations",
        aggregate=query_user_shares_from_user_operations(
            user_address=user_address,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            hypervisor_address=hypervisor_address,
        ),
    ):
        # check if user has balance or activity within the period
        if (
            userHype["first_shares_balance"] == "0"
            and userHype["last_shares_balance"] == "0"
            and not userHype["operations"]
        ):
            # this user has no activity and balance within the period. Skip
            logging.getLogger(__name__).debug(
                f"User {userHype['user_address']} has no activity within the period"
            )
            continue

        # create result structure
        if not userHype["user_address"] in users_result:
            users_result[userHype["user_address"]] = {}
        if not userHype["hypervisor_address"] in users_result[userHype["user_address"]]:
            users_result[userHype["user_address"]][userHype["hypervisor_address"]] = {
                "first_shares_balance": int(userHype["first_shares_balance"]),
                "last_shares_balance": int(userHype["last_shares_balance"]),
                "operations": 0,
                "twab_points": 0,
                "twab_percentage": 0,
            }
        if not userHype["hypervisor_address"] in hypervisors_totals:
            hypervisors_totals[userHype["hypervisor_address"]] = {
                "twab_points": 0,
                "operations": 0,
            }

        # define initial timestamp or block
        _time_key = "block" if block_ini else "timestamp"
        first_timeBlock = block_ini or timestamp_ini

        # calculate TWAB for the user
        for idx, operation in enumerate(userHype["operations"]):

            time_passed = operation[_time_key] - (
                first_timeBlock
                if idx == 0
                else userHype["operations"][idx - 1][_time_key]
            )

            # calculate TWAB
            _twab_points = time_passed * (
                int(userHype["first_shares_balance"])
                if idx == 0
                else int(userHype["operations"][idx - 1]["shares"]["balance"])
            )
            users_result[userHype["user_address"]][userHype["hypervisor_address"]][
                "twab_points"
            ] += _twab_points
            hypervisors_totals[userHype["hypervisor_address"]][
                "twab_points"
            ] += _twab_points
            users_result[userHype["user_address"]][userHype["hypervisor_address"]][
                "operations"
            ] += 1
            hypervisors_totals[userHype["hypervisor_address"]]["operations"] += 1

            # TODO: delete check on production
            if (
                int(userHype["first_shares_balance"])
                if idx == 0
                else int(userHype["operations"][idx - 1]["shares"]["balance"])
            ) + int(operation["shares"]["flow"]) != int(operation["shares"]["balance"]):
                # Just because balance is at the end of the block, check if there are more operations with the same block that match the balance
                if (
                    idx + 1 <= len(userHype["operations"]) - 1
                    and userHype["operations"][idx + 1]["block"] == operation["block"]
                ):
                    continue
                elif (
                    idx > 0
                    and userHype["operations"][idx - 1]["block"] == operation["block"]
                ):
                    continue
                # operations are missing
                logging.getLogger(__name__).error(
                    f"User {userHype['user_address']} shares are not correct. Operations are missing for hype {userHype['hypervisor_address']} between {_time_key}s {first_timeBlock if idx == 0 else userHype['operations'][idx - 1][_time_key]} and {operation[_time_key]}"
                )

    # calculate twab_percentage using hypervisors_totals
    _chekc_total_percentage = 0
    for user_address, hypervisors in users_result.items():
        for hypervisor_address, data in hypervisors.items():

            if (
                hypervisors_totals[hypervisor_address]["twab_points"] == 0
                and data["twab_points"] != 0
            ):
                logging.getLogger(__name__).error(
                    f"TWAB points for user {user_address} and hypervisor {hypervisor_address} is not correct. Total TWAB points for the hypervisor is 0"
                )
                continue
            elif hypervisors_totals[hypervisor_address]["twab_points"] == 0:
                # no twab points for this hypervisor and user. Skip
                continue

            data["twab_percentage"] = (
                data["twab_points"]
                / hypervisors_totals[hypervisor_address]["twab_points"]
            )
            _chekc_total_percentage += data["twab_percentage"]

    if _chekc_total_percentage != 1:
        logging.getLogger(__name__).error(
            f"Total TWAB percentage is not 1. Total is {_chekc_total_percentage}"
        )

    logging.getLogger(__name__).info(
        f" Gamma merkle rewards calculation took {time.time() - _startime:,.2f} seconds to complete"
    )

    return users_result
