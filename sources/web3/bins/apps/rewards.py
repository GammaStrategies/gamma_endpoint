import asyncio
from sources.common.general.enums import Chain, Dex, ChainId

from sources.web3.bins.configuration import STATIC_REGISTRY_ADDRESSES, RPC_URLS
from sources.web3.bins.w3.objects.protocols import zyberswap_masterchef_v1
from sources.web3.bins.w3.helpers import (
    build_zyberchef_anyRpc,
)
from sources.web3.bins.formulas.fin import calculate_rewards_apr

from sources.common.database.collection_endpoint import database_global, database_local

from sources.web3.bins.apps.prices import add_prices_to_hypervisor, get_token_price_usd
from sources.web3.bins.apps import hypervisors

# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


async def search_rewards_data_zyberswap(hypervisor_address: str, network: Chain):
    result = []
    # get the list of registry addresses
    for address in STATIC_REGISTRY_ADDRESSES[network.value]["zyberswap_v1_masterchefs"]:
        # create database connection

        # create zyberchef
        zyberchef = await build_zyberchef_anyRpc(
            address=address,
            network=network.value,
            block=0,
            rpcUrls=RPC_URLS[network],
            test=True,
        )

        # get the pool length
        poolLength = await zyberchef.poolLength
        for pid in range(poolLength):
            result += await get_rewards_data_zyberswap(
                hypervisor_address=hypervisor_address,
                network=network,
                pid=pid,
                zyberchef=zyberchef,
            )

    return result


async def get_rewards_data_zyberswap(
    hypervisor_address: str,
    network: Chain,
    pid: int,
    zyberchef_address: str | None = None,
    zyberchef: zyberswap_masterchef_v1 | None = None,
) -> list[dict]:
    result = []
    # create zyberchef
    if not zyberchef and zyberchef_address:
        zyberchef = zyberswap_masterchef_v1(
            address=zyberchef_address, network=network.value
        )
    elif not zyberchef and not zyberchef_address:
        raise Exception("zyberchef_address or zyberchef must be provided")

    #  lpToken address, allocPoint uint256, lastRewardTimestamp uint256, accZyberPerShare uint256, depositFeeBP uint16, harvestInterval uint256, totalLp uint256
    pinfo = await zyberchef.poolInfo(pid)

    if pinfo[0].lower() == hypervisor_address.lower():
        # this is the pid we are looking for

        # addresses address[], symbols string[], decimals uint256[], rewardsPerSec uint256[]
        poolRewardsPerSec = await zyberchef.poolRewardsPerSec(pid)
        # poolTotalLp = pinfo[6] / 10**18  # zyberchef.poolTotalLp(pid) # check

        # get rewards data
        rewards = {}
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


async def get_rewards(dex: Dex, hypervisor_address: str, network: Chain):
    result = []
    if dex == Dex.ZYBERSWAP:
        # get rewards data

        rewards_data = await search_rewards_data_zyberswap(
            hypervisor_address=hypervisor_address, network=network
        )
        hypervisor_data = await hypervisors.get_hypervisor_data(
            network=network,
            dex=dex,
            hypervisor_address=hypervisor_address,
            convert_to_decimal=True,
        )

        # add prices to hypervisor
        (
            hypervisor_data["token0_price_usd"],
            hypervisor_data["token1_price_usd"],
        ) = await asyncio.gather(
            get_token_price_usd(
                token_address=hypervisor_data["token0_address"].lower(),
                network=network.value,
                block=0,
            ),
            get_token_price_usd(
                token_address=hypervisor_data["token1_address"].lower(),
                network=network.value,
                block=0,
            ),
        )

        hypervisor_data["lpToken_price_usd"] = (
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

        for rewards in rewards_data:
            # calculate rewards APR
            apr = calculate_rewards_apr(
                token_price=await get_token_price_usd(
                    token_address=rewards["rewardToken"].lower(),
                    network=network.value,
                    block=0,
                ),
                token_decimals=rewards["rewardToken_decimals"],
                token_reward_rate=rewards["poolRewardsPerSec"],
                total_lp_locked=(
                    rewards["poolTotalLp"] / (10 ** hypervisor_data["decimals"])
                ),
                lp_token_price=hypervisor_data["lpToken_price_usd"],
            )
            result.append(
                {
                    **rewards,
                    "apr": apr,
                    "token0_price_usd": hypervisor_data["token0_price_usd"],
                    "token1_price_usd": hypervisor_data["token1_price_usd"],
                    "lpToken_price_usd": hypervisor_data["lpToken_price_usd"],
                }
            )

    return result
