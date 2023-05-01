from sources.common.general.enums import Chain, Dex, ChainId
import asyncio

from sources.web3.bins.w3.helpers import (
    build_hypervisor,
    build_hypervisor_anyRpc,
    build_hypervisor_registry,
    build_hypervisor_registry_anyRpc,
    build_zyberchef_anyRpc,
)

from sources.web3.bins.w3.objects.protocols import gamma_hypervisor
from sources.web3.bins.w3.objects.exchanges import univ3_pool, algebrav3_pool
from sources.web3.bins.configuration import RPC_URLS, CONFIGURATION
from sources.web3.bins.mixed.price_utilities import price_scraper


async def hypervisors_list(network: Chain, dex: Dex):
    # get network registry address
    registry = await build_hypervisor_registry_anyRpc(
        network=network, dex=dex, block=0, rpcUrls=RPC_URLS[network.value], test=True
    )

    return await registry.get_hypervisors_addresses()


async def hypervisor_uncollected_fees(
    network: Chain, dex: Dex, hypervisor_address: str, block: int = None
):
    hypervisor = await build_hypervisor_anyRpc(
        network=network,
        dex=dex,
        hypervisor_address=hypervisor_address,
        block=block if block else 0,
        rpcUrls=RPC_URLS[network.value],
        test=True,
    )

    block, timestamp = await hypervisor.init_block()

    # get property vars
    symbol, baseUpper, baseLower, limitUpper, limitLower, pool = await asyncio.gather(
        hypervisor.symbol,
        hypervisor.baseUpper,
        hypervisor.baseLower,
        hypervisor.limitUpper,
        hypervisor.limitLower,
        hypervisor.pool,
    )

    base, limit = await asyncio.gather(
        pool.get_fees_uncollected(
            ownerAddress=hypervisor.address,
            tickUpper=baseUpper,
            tickLower=baseLower,
            inDecimal=True,
        ),
        pool.get_fees_uncollected(
            ownerAddress=hypervisor.address,
            tickUpper=limitUpper,
            tickLower=limitLower,
            inDecimal=True,
        ),
    )

    totalFees0 = (
        float(base["qtty_token0"])
        + float(base["qtty_token0_owed"])
        + float(limit["qtty_token0"])
        + float(limit["qtty_token0_owed"])
    )
    totalFees1 = (
        float(base["qtty_token1"])
        + float(base["qtty_token1_owed"])
        + float(limit["qtty_token1"])
        + float(limit["qtty_token1_owed"])
    )

    return {
        "block": block,
        "timestamp": timestamp,
        "symbol": symbol,
        "baseFees0": float(base["qtty_token0"]),
        "baseFees1": float(base["qtty_token1"]),
        "baseTokensOwed0": float(base["qtty_token0_owed"]),
        "baseTokensOwed1": float(base["qtty_token1_owed"]),
        "limitFees0": float(limit["qtty_token0"]),
        "limitFees1": float(limit["qtty_token1"]),
        "limitTokensOwed0": float(limit["qtty_token0_owed"]),
        "limitTokensOwed1": float(limit["qtty_token1_owed"]),
        # "baseFees0USD": float(base[0]) * hypervisor.baseTokenPrice,
        # "baseFees1USD": float(base[1]) * hypervisor.quoteTokenPrice,
        # "baseTokensOwed0USD": float(base[2]) * hypervisor.baseTokenPrice,
        # "baseTokensOwed1USD": float(base[3]) * hypervisor.quoteTokenPrice,
        # "limitFees0USD": float(limit[0]) * hypervisor.baseTokenPrice,
        # "limitFees1USD": float(limit[1]) * hypervisor.quoteTokenPrice,
        # "limitTokensOwed0USD": float(limit[2]) * hypervisor.baseTokenPrice,
        # "limitTokensOwed1USD": float(limit[3]) * hypervisor.quoteTokenPrice,
        "totalFees0": totalFees0,
        "totalFees1": totalFees1,
        # "totalFeesUSD": (float(base[0]) + float(limit[0])) * hypervisor.baseTokenPrice + (float(base[1]) + float(limit[1])) * hypervisor.quoteTokenPrice,
    }


async def get_hypervisor_data(
    network: Chain, dex: Dex, hypervisor_address: str, convert_to_decimal: bool = False
):
    if hypervisor := await build_hypervisor_anyRpc(
        network=network,
        dex=dex,
        block=0,
        hypervisor_address=hypervisor_address,
        rpcUrls=RPC_URLS[network.value],
        test=True,
    ):
        block, timestamp = await hypervisor.init_block()
        (
            totalSupply,
            totalAmounts,
            hypervisor_decimals,
            token0,
            token1,
        ) = await asyncio.gather(
            hypervisor.totalSupply,
            hypervisor.getTotalAmounts,
            hypervisor.decimals,
            hypervisor.token0,
            hypervisor.token1,
        )

        (
            token0_decimals,
            token1_decimals,
        ) = await asyncio.gather(token0.decimals, token1.decimals)
        token0_address = token0.address
        token1_address = token1.address

        if convert_to_decimal:
            totalSupply = totalSupply / 10**hypervisor_decimals
            totalAmounts["total0"] = totalAmounts["total0"] / 10**token0_decimals
            totalAmounts["total1"] = totalAmounts["total1"] / 10**token1_decimals

        return {
            "block": block,
            "timestamp": timestamp,
            "address": hypervisor_address,
            "token0_address": token0_address,
            "token1_address": token1_address,
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "decimals": hypervisor_decimals,
            "totalSupply": totalSupply,
            "totalAmounts": totalAmounts,
        }
