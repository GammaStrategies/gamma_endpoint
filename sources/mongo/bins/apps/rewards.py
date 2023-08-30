import logging
from sources.common.general.enums import Chain, Protocol
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

    return result
