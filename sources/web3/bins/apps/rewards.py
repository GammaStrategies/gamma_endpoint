import asyncio
from sources.common.general.enums import Chain, Dex

from sources.web3.bins.configuration import STATIC_REGISTRY_ADDRESSES
from sources.web3.bins.w3.objects.rewarders import (
    zyberswap_masterchef_v1,
    thena_voter_v3,
    thena_gauge_V2,
)
from sources.web3.bins.w3.objects.basic import erc20
from sources.web3.bins.formulas.fin import calculate_rewards_apr


from sources.web3.bins.apps.prices import get_token_price_usd
from sources.web3.bins.apps import hypervisors


async def search_rewards_data_zyberswap(
    hypervisor_address: str, network: Chain, block: int | None = None
):
    result = []

    # get the list of registry addresses
    for address in STATIC_REGISTRY_ADDRESSES[network.database_name][
        "zyberswap_v1_masterchefs"
    ]:
        # create database connection

        # create zyberchef
        zyberchef = zyberswap_masterchef_v1(
            address=address,
            network=network,
            block=block or 0,
        )

        # get the pool length
        poolLength = await zyberchef.poolLength
        for pid in range(poolLength):
            result += await get_rewards_data_zyberswap(
                hypervisor_address=hypervisor_address,
                network=network,
                pid=pid,
                zyberchef=zyberchef,
                block=block,
            )

    return result


async def get_rewards_data_zyberswap(
    hypervisor_address: str,
    network: Chain,
    pid: int,
    zyberchef_address: str | None = None,
    zyberchef: zyberswap_masterchef_v1 | None = None,
    block: int | None = None,
) -> list[dict]:
    result = []

    # create zyberchef
    if not zyberchef and zyberchef_address:
        zyberchef = zyberswap_masterchef_v1(
            address=zyberchef_address, network=network.database_name, block=block or 0
        )
    elif not zyberchef and not zyberchef_address:
        raise Exception("zyberchef_address or zyberchef must be provided")

    #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
    pinfo = await zyberchef.poolInfo(pid)

    if pinfo[0].lower() == hypervisor_address.lower():
        # this is the pid we are looking for

        # allocPoint = pinfo[1]
        # total_allocPoint = await zyberchef.totalAllocPoint

        # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
        poolRewardsPerSec = await zyberchef.poolRewardsPerSec(pid)  # / (10**decimals)

        # get rewards data
        for address, symbol, decimals, rewardsPerSec in zip(
            poolRewardsPerSec[0],
            poolRewardsPerSec[1],
            poolRewardsPerSec[2],
            poolRewardsPerSec[3],
        ):
            if rewardsPerSec:
                result.append(
                    {
                        "network": network.value,
                        "block": await zyberchef.block,
                        "timestamp": await zyberchef.timestamp,
                        "hypervisor_address": hypervisor_address,
                        "rewardToken": address,
                        "rewardToken_symbol": symbol,
                        "rewardToken_decimals": decimals,
                        "poolRewardsPerSec": rewardsPerSec,
                        "poolTotalLp": pinfo[6],
                    }
                )

    return result


async def get_rewards_data_thena(
    hypervisor_address: str,
    network: Chain,
    block: int | None = None,
):
    if voter_url := STATIC_REGISTRY_ADDRESSES.get(network.database_name, {}).get(
        "thena_voter", None
    ):
        # build thena voter
        thena_voter = thena_voter_v3(
            address=voter_url, network=network.database_name, block=block or 0
        )

        # get managing gauge from hype address
        gauge_address = await thena_voter.gauges(address=hypervisor_address)

        # build thena gauge instance
        thena_gauge = thena_gauge_V2(
            address=gauge_address,
            network=network.database_name,
            block=block or 0,
        )
        # get gauge data
        rewardRate, rewardToken, totalSupply, block = await asyncio.gather(
            thena_gauge.rewardRate,
            thena_gauge.rewardToken,
            thena_gauge.totalSupply,
            thena_gauge.block,
        )

        # build reward token instance
        reward_token_instance = erc20(
            address=rewardToken,
            network=network.database_name,
            block=block or 0,
        )
        # get reward token data
        rewardToken_symbol, rewardToken_decimals = await asyncio.gather(
            reward_token_instance.symbol, reward_token_instance.decimals
        )

        # return data
        return {
            "network": network,
            "block": block,
            "timestamp": await thena_gauge.timestamp,
            "hypervisor_address": hypervisor_address.lower(),
            "rewarder_address": gauge_address.lower(),
            "rewarder_type": "thena_gauge",
            "rewarder_refIds": [-1],
            "rewardToken": rewardToken.lower(),
            "rewardToken_symbol": rewardToken_symbol,
            "rewardToken_decimals": rewardToken_decimals,
            "poolRewardsPerSec": rewardRate,
            "poolTotalLp": totalSupply,
        }


async def get_rewards(
    dex: Dex, hypervisor_address: str, network: Chain, block: int | None = None
):
    result = []

    # retrieve hypervisor related data
    hypervisor_data = await hypervisors.get_hypervisor_data_for_rewards(
        network=network,
        dex=dex,
        hypervisor_address=hypervisor_address,
        block=block,
        convert_to_decimal=True,
    )
    # add prices hype
    (
        hypervisor_data["token0_price_usd"],
        hypervisor_data["token1_price_usd"],
    ) = await asyncio.gather(
        get_token_price_usd(
            token_address=hypervisor_data["token0_address"].lower(),
            network=network.database_name,
            block=block or 0,
        ),
        get_token_price_usd(
            token_address=hypervisor_data["token1_address"].lower(),
            network=network.database_name,
            block=block or 0,
        ),
    )
    # add share price
    hypervisor_data["hypervisor_share_price_usd"] = (
        (
            (
                hypervisor_data["token0_price_usd"]
                * (int(hypervisor_data["totalAmounts"]["total0"]))
                + hypervisor_data["token1_price_usd"]
                * (int(hypervisor_data["totalAmounts"]["total1"]))
            )
            / hypervisor_data["totalSupply"]
        )
        if hypervisor_data["totalSupply"]
        else 0
    )

    # choose the right reward data
    if dex == Dex.ZYBERSWAP:
        # get rewards data
        rewards_data = await search_rewards_data_zyberswap(
            hypervisor_address=hypervisor_address, network=network
        )

        for rewards in rewards_data:
            # get rwrd token price
            rewardToken_price = await get_token_price_usd(
                token_address=rewards["rewardToken"].lower(),
                network=network.database_name,
                block=block or 0,
            )
            # convert to decimals
            converted_rewardPoolRewardsPerSec = rewards["poolRewardsPerSec"] / (
                10 ** rewards["rewardToken_decimals"]
            )
            converted_total_lp_locked = rewards["poolTotalLp"] / (
                10 ** hypervisor_data["decimals"]
            )

            # calculate rewards APR
            apr = calculate_rewards_apr(
                token_price=rewardToken_price,
                token_reward_rate=converted_rewardPoolRewardsPerSec,
                total_lp_locked=converted_total_lp_locked,
                lp_token_price=hypervisor_data["hypervisor_share_price_usd"],
            )
            result.append(
                {
                    "hypervisor_symbol": hypervisor_data["symbol"],
                    "hypervisor_address": hypervisor_address,
                    "dex": dex,
                    "apr": apr,
                    "rewardToken_price_usd": rewardToken_price,
                    "token0_price_usd": hypervisor_data["token0_price_usd"],
                    "token1_price_usd": hypervisor_data["token1_price_usd"],
                    "hypervisor_share_price_usd": hypervisor_data[
                        "hypervisor_share_price_usd"
                    ],
                    "rewardsData": {
                        **rewards,
                        "converted_poolRewardsPerSec": converted_rewardPoolRewardsPerSec,
                        "converted_total_lp_locked": converted_total_lp_locked,
                    },
                }
            )

    elif dex == Dex.THENA:
        rewards_data = await get_rewards_data_thena(
            hypervisor_address=hypervisor_address, network=network, block=block
        )
        # get rwrd token price
        rewardToken_price = await get_token_price_usd(
            token_address=rewards_data["rewardToken"].lower(),
            network=network.database_name,
            block=block or 0,
        )

        # convert to decimals
        converted_rewardPoolRewardsPerSec = rewards_data["poolRewardsPerSec"] / (
            10 ** rewards_data["rewardToken_decimals"]
        )
        converted_total_lp_locked = rewards_data["poolTotalLp"] / (
            10 ** hypervisor_data["decimals"]
        )

        # calculate rewards APR
        apr = calculate_rewards_apr(
            token_price=rewardToken_price,
            token_reward_rate=converted_rewardPoolRewardsPerSec,
            total_lp_locked=converted_total_lp_locked,
            lp_token_price=hypervisor_data["hypervisor_share_price_usd"],
        )
        result.append(
            {
                "hypervisor_symbol": hypervisor_data["symbol"],
                "hypervisor_address": hypervisor_address,
                "dex": dex,
                "apr": apr,
                "rewardToken_price_usd": rewardToken_price,
                "token0_price_usd": hypervisor_data["token0_price_usd"],
                "token1_price_usd": hypervisor_data["token1_price_usd"],
                "hypervisor_share_price_usd": hypervisor_data[
                    "hypervisor_share_price_usd"
                ],
                "rewardsData": {
                    **rewards_data,
                    "converted_poolRewardsPerSec": converted_rewardPoolRewardsPerSec,
                    "converted_total_lp_locked": converted_total_lp_locked,
                },
            }
        )

    return result
