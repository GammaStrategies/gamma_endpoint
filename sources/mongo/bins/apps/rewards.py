import asyncio
import logging
from sources.common.general.enums import Chain, Protocol
from sources.common.xt_api.ramses import ramses_api_helper
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

    return await rewrite_mfd_with_api(result, network)


async def rewrite_mfd_with_api(data: dict, chain: Chain) -> dict:

    try:
        ramses_api = ramses_api_helper(chain=chain)
        if not ramses_api:
            logging.getLogger(__name__).warning("Ramses API not available")
            return data

        tmp, hypervisors_static = await asyncio.gather(
            ramses_api.data(),
            local_database_helper(network=chain).get_items_from_database(
                collection_name="static",
                find={},
                projection={"_id": 0, "address": 1, "symbol": 1, "pool": 1},
            ),
        )
        hypervisors_static = {x["address"]: x for x in hypervisors_static}

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
                            f"Error getting ramses api data: {e}"
                        )
                        continue

                    #### modify rewarder data
                    ####
                    rewarder_data["rewardPerSecond"] = tmp_data["rewardsPerSecond"]
                    rewarder_data["apr"] = tmp_data["apr"]
                    rewarder_data["baseApr"] = tmp_data["apr"]
                    rewarder_data["boostApr"] = 0
                    rewarder_data["baseRewardPerSecond"] = tmp_data["rewardsPerSecond"]
                    rewarder_data["boostRewardPerSecond"] = 0

                    #### modify totals
                    ####
                    hype_data["stakedAmountUSD"] = tmp_data["totalValueLockedUSD"]
                    # hype_data["stakedAmount"] =
                    hype_data["apr"] += tmp_data["apr"]
                    hype_data["baseApr"] += tmp_data["apr"]
                    hype_data["boostApr"] += 0

    except Exception as e:
        logging.getLogger(__name__).error(f"Error rewriting mfd with api: {e}")

    return data
