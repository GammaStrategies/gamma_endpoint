import asyncio
from datetime import datetime, timezone
import logging
import time
from sources.common.database.collection_endpoint import database_global
from sources.common.general.enums import Chain, Protocol
from sources.common.xt_api.ramses import ramses_api_helper
from sources.internal.bins.user import query_user_shares_from_user_operations
from sources.mongo.bins.apps.user import get_user_operations
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
    if not network == Chain.LINEA:
        return await rewrite_mfd_with_latest_reward_snapshots(result, network)
    # return await rewrite_mfd_with_api(result, network, protocol)
    # return await rewrite_mfd_with_custom(result, network)
    else:
        return result


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
                            f"Error getting ramses api data for pool {hypervisors_static[hype_address]['pool']['address']}: {e}"
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
                        rewarder_data["emissionRate"] = tmp_data["rewardRate"]
                        rewarder_data["emissionRateUSD"] = tmp_data["usdPerSecond"]
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


async def rewrite_mfd_with_latest_reward_snapshots(data: dict, chain: Chain):

    latest_rewards_by_hype = {}
    try:
        for item in await retrieve_rewards_from_latest_snapshots(chain=chain):
            # add hypervisor address to dict
            if not item["hypervisor_address"] in latest_rewards_by_hype:
                latest_rewards_by_hype[item["hypervisor_address"]] = {}
            # add rewardToken to dict
            if (
                not item["rewardToken"]
                in latest_rewards_by_hype[item["hypervisor_address"]]
            ):
                latest_rewards_by_hype[item["hypervisor_address"]][
                    item["rewardToken"]
                ] = item
            else:
                logging.getLogger(__name__).error(
                    f" rewardToken {item['rewardToken']} already in latest_rewards_by_hype[{item['hypervisor_address']}]"
                )

        if not latest_rewards_by_hype:
            logging.getLogger(__name__).warning(
                f"No rewards apr from hype returns available for {chain.fantasy_name}"
            )
            return data

        items_to_remove = []
        for mfd_address, item in data.items():
            for hype_address, hype_data in item["hypervisors"].items():

                if hype_address not in latest_rewards_by_hype:
                    logging.getLogger(__name__).warning(
                        f"No rewards apr from hype returns available for {hype_address} in {chain.fantasy_name}-. Removing from result"
                    )
                    # remove ?
                    items_to_remove.append(mfd_address)
                    continue

                # reset hypervisor totals
                hype_data["apr"] = 0
                hype_data["baseApr"] = 0
                hype_data["boostApr"] = 0

                # reset hypervisor rewarders
                for rewardToken_address, rewarder_data in hype_data[
                    "rewarders"
                ].items():

                    # get the reward token from the latest snapshot
                    if rewardToken_address in latest_rewards_by_hype[hype_address]:
                        # modify rewarder data
                        rewarder_data["timestamp"] = latest_rewards_by_hype[
                            hype_address
                        ][rewardToken_address]["timestamp"]

                        rewarder_data["rewardPerSecond"] = latest_rewards_by_hype[
                            hype_address
                        ][rewardToken_address]["rewards_perSecond"]

                        rewarder_data["apr"] = latest_rewards_by_hype[hype_address][
                            rewardToken_address
                        ]["apr"]
                        rewarder_data["baseApr"] = rewarder_data["apr"]
                        rewarder_data["boostApr"] = 0
                        rewarder_data["baseRewardPerSecond"] = rewarder_data[
                            "rewardPerSecond"
                        ]
                        rewarder_data["boostRewardPerSecond"] = 0

                        # modify totals
                        hype_data["apr"] += rewarder_data["apr"]
                        hype_data["baseApr"] += rewarder_data["apr"]
                        # hype_data["boostApr"] = 0

                        # pop item from latest_rewards_by_hype
                        latest_rewards_by_hype[hype_address].pop(rewardToken_address)

                # add the remaining rewarders from the latest rewards snapshots
                for rewardToken_address, rewarder_data in latest_rewards_by_hype[
                    hype_address
                ].items():
                    hype_data["rewarders"][rewardToken_address] = {
                        "timestamp": rewarder_data["timestamp"],
                        "rewardToken": rewardToken_address,
                        "rewardTokenDecimals": rewarder_data["rewardTokenDecimals"],
                        "rewardTokenSymbol": rewarder_data["rewardTokenSymbol"],
                        "rewardPerSecond": rewarder_data["rewards_perSecond"],
                        "apr": rewarder_data["apr"],
                        "baseApr": rewarder_data["apr"],
                        "boostApr": 0,
                        "baseRewardPerSecond": rewarder_data["rewards_perSecond"],
                        "boostRewardPerSecond": 0,
                    }

                    # modify totals
                    hype_data["apr"] += rewarder_data["apr"]
                    hype_data["baseApr"] += rewarder_data["apr"]
                    # hype_data["boostApr"] = 0

        # remove items
        for item in items_to_remove:
            data.pop(item)

    except Exception as e:
        logging.getLogger(__name__).exception(
            f"Error rewriting mfd with latest reward snapshots: {e}"
        )

    return data


# Rewards from latest snapshots
async def retrieve_rewards_from_latest_snapshots(
    chain: Chain,
    hypervisor_addresses: list[str] | None = None,
) -> list[dict]:
    """Retrieve rewards from the latest rewards snapshots collection

    Args:
        chain (Chain):
        hypervisor_addresses (list[str] | None, optional): . Defaults to All available.

    Returns:
        list[dict]:
    """

    # get
    return await local_database_helper(network=chain).get_items_from_database(
        collection_name="latest_reward_snapshots",
        find=(
            {}
            if not hypervisor_addresses
            else {"hypervisor_address": {"$in": hypervisor_addresses}}
        ),
    )


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


async def gamma_rewards_TWA_calculation(
    chain: Chain,
    protocol: Protocol | None = None,
    hypervisors: list[str] | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):
    # one of block or timestamp should be provided ( including end)

    # decide whether to use timestamp or block
    timevar_txt = "timestamp"
    timevar_ini = timestamp_ini
    timevar_end = timestamp_end
    if block_ini:
        timevar_txt = "block"
        timevar_ini = block_ini
        timevar_end = block_end

    # get user data ( do not filter by user yet as we need all users to calculate TWA)
    user_hypervisor_data = await get_user_operations(
        chain=chain,
        protocol=protocol,
        hypervisor_address_list=hypervisors,
        block_ini=block_ini,
        block_end=block_end,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
        # important to include zero balances
        return_zero_balance=True,
    )
    if not user_hypervisor_data:
        return "No user data found"

    # hypervisor helper variable {
    #             <hype_address>: {
    #                   "users": { <user>: { "initial_balance": <initial_balance>, "final_balance": <final_balance>, "operations":[]}}
    #                   f"{timevar_txt}s":{ <block/timestamp>: <hypervisor_status> } }
    #                       }
    hype_data = {}
    for user_hype_item in user_hypervisor_data:
        # check if hypervisor is in the dict
        if user_hype_item["hypervisor_address"] not in hype_data:
            hype_data[user_hype_item["hypervisor_address"]] = {
                f"{timevar_txt}_ini": timevar_ini,
                f"{timevar_txt}_end": timevar_end,
                "total_twa": 0,
                "total_twa_percent": 0,
                "users": {
                    user_hype_item["user"]: {
                        "initial_balance": 0,
                        "final_balance": 0,
                        "twa": 0,
                        "twa_percent": 0,
                        "operations": [],
                    }
                },
                f"{timevar_txt}s": {},
            }
        else:
            # check if user is in the dict
            if (
                user_hype_item["user"]
                not in hype_data[user_hype_item["hypervisor_address"]]["users"]
            ):
                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ] = {
                    "initial_balance": 0,
                    "final_balance": 0,
                    "twa": 0,
                    "twa_percent": 0,
                    "operations": [],
                }

        last_time = timevar_ini
        for operation in user_hype_item["operations"]:

            # check if block/timestamp is in the dict
            if (
                operation[timevar_txt]
                not in hype_data[user_hype_item["hypervisor_address"]][
                    f"{timevar_txt}s"
                ]
            ):
                hype_data[user_hype_item["hypervisor_address"]][f"{timevar_txt}s"][
                    operation[timevar_txt]
                ] = operation["hypervisor_status"]

            # check if block/timestamp is lower than initial block/timestamp
            if operation[timevar_txt] < timevar_ini:
                # this is the user's initial balance.
                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["initial_balance"] = operation["shares"]["balance"]
                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["final_balance"] = operation["shares"]["balance"]
            else:
                # total supply before the operation
                hype_totalSupply_denominator = (
                    operation["hypervisor_status"]["totalSupply"]
                    + operation["shares"]["flow"]
                )

                user_twa_numerator = calculate_twa(
                    hype_data[user_hype_item["hypervisor_address"]]["users"][
                        user_hype_item["user"]
                    ]["final_balance"],
                    hype_totalSupply_denominator,
                    operation[timevar_txt],
                    last_time,
                )

                # append operation to the operations list
                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["operations"].append(
                    {
                        f"{timevar_txt}": operation[timevar_txt],
                        "time_passed": operation[timevar_txt] - last_time,
                        "totalSupply": hype_totalSupply_denominator,
                        "balance": hype_data[user_hype_item["hypervisor_address"]][
                            "users"
                        ][user_hype_item["user"]]["final_balance"],
                        "twa": user_twa_numerator,
                    }
                )

                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["twa"] += user_twa_numerator

                hype_data[user_hype_item["hypervisor_address"]][
                    "total_twa"
                ] += user_twa_numerator

                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["twa_percent"] = hype_data[user_hype_item["hypervisor_address"]][
                    "users"
                ][
                    user_hype_item["user"]
                ][
                    "twa"
                ] / (
                    timevar_end - timevar_ini
                )

                hype_data[user_hype_item["hypervisor_address"]]["total_twa_percent"] = (
                    hype_data[user_hype_item["hypervisor_address"]]["total_twa"]
                    / (timevar_end - timevar_ini)
                )

                # this is an operation after the initial block/timestamp
                # set final balance as current balance
                hype_data[user_hype_item["hypervisor_address"]]["users"][
                    user_hype_item["user"]
                ]["final_balance"] = operation["shares"]["balance"]
                # append operation to the operations list
                # hype_data[user_hype_item["hypervisor_address"]]["users"][
                #     user_hype_item["user"]
                # ]["operations"].append(operation)
                last_time = operation[timevar_txt]
    return hype_data
    # calculate TWA from hype_data created
    for hype_address, hype_data_item in hype_data.items():
        # get initial/end hypervisor supply ( filter blocks lower than initial block from f"{timevar_txt}s" and get the max one)
        initial_hypervisor_supply = max(
            [
                status["totalSupply"]
                for block, status in hype_data_item[f"{timevar_txt}s"].items()
                if block < timevar_ini
            ]
        )
        end_hypervisor_supply = max(
            [
                status["totalSupply"]
                for block, status in hype_data_item[f"{timevar_txt}s"].items()
                if block > timevar_ini
            ]
        )
        hype_data_item["initial_hypervisor_supply"] = initial_hypervisor_supply
        hype_data_item["end_hypervisor_supply"] = end_hypervisor_supply
        hype_data_item["total_twa"] = 0

        # delete_zero balances
        users_to_remove = []
        for user_address, user_data in hype_data_item["users"].items():
            last_time = timevar_ini
            user_twa_numerator = 0
            # denominator is time1 - time0
            # check if no block_end or timestamp_end, use last block or timestamp from f"{timevar_txt}s"
            timevar_end = (
                block_end
                or timestamp_end
                or max(list(hype_data_item[f"{timevar_txt}s"].keys()))
            )
            user_twa_denominator = timevar_end - timevar_ini
            # loop thu all operations
            for operation in user_data["operations"]:

                # this operation should be after the initial block/timestamp
                if operation[timevar_txt] < timevar_ini:
                    # error should never happen
                    continue

                # if this is the first operation, use the initial balance
                if last_time == timevar_ini:
                    user_twa_numerator += calculate_twa(
                        user_data["initial_balance"],
                        initial_hypervisor_supply,
                        operation[timevar_txt],
                        last_time,
                    )
                else:
                    user_twa_numerator += calculate_twa(
                        operation["shares"]["balance"] - operation["shares"]["flow"],
                        operation["hypervisor_status"]["totalSupply"],
                        operation[timevar_txt],
                        last_time,
                    )

                # change last time
                last_time = operation[timevar_txt]

            # calculate final step ( from last operation to end)
            # check if last operation is the same as the initial block/timestamp
            if last_time == timevar_ini:
                # if so, use initial balance
                user_twa_numerator += calculate_twa(
                    user_data["final_balance"],
                    end_hypervisor_supply,
                    timevar_end,
                    last_time,
                )
            else:
                # else, use final balance
                user_twa_numerator += calculate_twa(
                    user_data["final_balance"],
                    end_hypervisor_supply,
                    timevar_end,
                    last_time,
                )
            last_time = timevar_end

            # calculate TWA
            user_twa = user_twa_numerator / user_twa_denominator
            # add to user_data
            user_data["twa"] = user_twa
            # add to total_twa
            hype_data_item["total_twa"] += user_twa

            # check if user_twa is zero
            if user_twa == 0:
                users_to_remove.append(user_address)

        # remove users with zero TWA
        for user_address in users_to_remove:
            hype_data_item["users"].pop(user_address)
        # remove f"{timevar_txt}s" from hype_data_item
        hype_data_item.pop(f"{timevar_txt}s")

    # by adding all twas

    return hype_data


def calculate_twa(b, s, t1, t0):
    """Helper function to calculate TWA

    Args:
        b :
        s :
        t1:
        t0:

    Returns:

    """
    return (b / s) * (t1 - t0)


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
