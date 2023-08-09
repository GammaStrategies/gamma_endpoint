import logging
from sources.common.general.enums import Chain, Protocol
from sources.mongo.bins.apps.hypervisor import local_database_helper


# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


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
        stakedAmount = int(item.get("last_updated_data", {}).get("total_staked", 0)) / (
            10 ** item.get("hypervisor_static", {}).get("decimals", 0)
        )
        stakedAmountUSD = stakedAmount * item.get("last_updated_data", {}).get(
            "hypervisor_price_x_share", 0
        )

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
            #  <token_address>:{ "rewardToken": "", "rewardTokenDecimals": 0, "rewardTokenSymbol":"", "rewardPerSecond":0, apr:0,}

        # vars
        seconds_elapsed = int(item["timestamp"]) - int(
            item["last_updated_data"]["timestamp"]
        )

        baseRewards = int(
            item.get("current_period_rewards", {}).get("current_baseRewards", 0)
        ) - int(
            item.get("last_updated_data", {})
            .get("current_period_rewards", {})
            .get("current_baseRewards", 0)
        )
        boostRewards = int(
            item.get("current_period_rewards", {}).get("current_boostRewards", 0)
        ) - int(
            item.get("last_updated_data", {})
            .get("current_period_rewards", {})
            .get("current_boostRewards", 0)
        )
        baseRewardPerSecond = baseRewards / seconds_elapsed
        boostRewardPerSecond = boostRewards / seconds_elapsed
        if baseRewardPerSecond.is_integer():
            baseRewardPerSecond = int(baseRewardPerSecond)
        if boostRewardPerSecond.is_integer():
            boostRewardPerSecond = int(boostRewardPerSecond)

        # calculate apr
        baseApr = (
            (
                (
                    (baseRewardPerSecond / (10 ** item["rewardToken_decimals"]))
                    * 31536000
                    * item["rewardToken_price"]
                )
                / stakedAmountUSD
            )
            if stakedAmountUSD
            else 0
        )

        boostApr = (
            (
                (
                    (boostRewardPerSecond / (10 ** item["rewardToken_decimals"]))
                    * 31536000
                    * item["rewardToken_price"]
                )
                / stakedAmountUSD
            )
            if stakedAmountUSD
            else 0
        )

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
                "lastRewardTimestamp": item["last_updated_data"]["timestamp"],
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

    # # convert total apr in string
    # for mfd in result:
    #     for hypervisor in result[mfd]["hypervisors"]:
    #         result[mfd]["hypervisors"][hypervisor]["apr"] = result[mfd]["hypervisors"][
    #             hypervisor
    #         ]["apr"]

    #         result[mfd]["hypervisors"][hypervisor]["baseApr"] = result[mfd][
    #             "hypervisors"
    #         ][hypervisor]["baseApr"]

    #         result[mfd]["hypervisors"][hypervisor]["boostApr"] = result[mfd][
    #             "hypervisors"
    #         ][hypervisor]["boostApr"]

    return result
