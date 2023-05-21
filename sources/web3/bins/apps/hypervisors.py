from sources.common.general.enums import Chain, Dex, ChainId
import asyncio
from sources.mongo.bins.enums import enumsConverter
from sources.web3.bins.general.general_utilities import async_rgetattr

from sources.web3.bins.w3.helpers import (
    build_hypervisor,
    build_hypervisor_registry,
)
from sources.web3.bins.w3.objects.basic import web3wrap

from sources.web3.bins.w3.objects.protocols import gamma_hypervisor
from sources.web3.bins.w3.objects.exchanges import univ3_pool, algebrav3_pool
from sources.web3.bins.configuration import RPC_URLS, CONFIGURATION
from sources.web3.bins.mixed.price_utilities import price_scraper


async def hypervisors_list(network: Chain, dex: Dex):
    netval = enumsConverter.convert_general_to_local(chain=network).value

    # get network registry address
    registry = build_hypervisor_registry(
        network=network,
        dex=dex,
        block=0,
    )

    return await registry.get_hypervisors_addresses()


async def hypervisor_uncollected_fees(
    network: Chain, dex: Dex, hypervisor_address: str, block: int = None
):
    netval = enumsConverter.convert_general_to_local(chain=network).value

    if hypervisor := build_hypervisor(
        network=network,
        dex=dex,
        block=block or 0,
        hypervisor_address=hypervisor_address,
    ):
        block, timestamp = await hypervisor.init_block()

        # get property vars
        (
            symbol,
            baseUpper,
            baseLower,
            limitUpper,
            limitLower,
            pool,
        ) = await asyncio.gather(
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


async def get_hypervisor_data_for_rewards(
    network: Chain, dex: Dex, hypervisor_address: str, convert_to_decimal: bool = False
):
    netval = enumsConverter.convert_general_to_local(chain=network).value
    if hypervisor := build_hypervisor(
        network=network,
        dex=dex,
        block=0,
        hypervisor_address=hypervisor_address,
    ):
        block, timestamp = await hypervisor.init_block()
        (
            totalSupply,
            totalAmounts,
            hypervisor_decimals,
            token0,
            token1,
            symbol,
        ) = await asyncio.gather(
            hypervisor.totalSupply,
            hypervisor.getTotalAmounts,
            hypervisor.decimals,
            hypervisor.token0,
            hypervisor.token1,
            hypervisor.symbol,
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
            "symbol": symbol,
            "address": hypervisor_address,
            "token0_address": token0_address,
            "token1_address": token1_address,
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "decimals": hypervisor_decimals,
            "totalSupply": totalSupply,
            "totalAmounts": totalAmounts,
        }


async def get_hypervisor_data(
    network: Chain,
    dex: Dex,
    hypervisor_address: str,
    fields: list[str],
    block: int | None = None,
):
    netval = enumsConverter.convert_general_to_local(chain=network).value

    hype = build_hypervisor(
        network=network,
        dex=dex,
        block=block or 0,
        hypervisor_address=hypervisor_address,
    )
    callables = ["as_dict", "get_tvl", "get_fees_uncollected", "get_qtty_depoloyed"]

    _block, _timestamp = await hype.init_block()

    _results = await asyncio.gather(
        *[async_rgetattr(hype, field, callables=callables) for field in fields]
    )

    fields.append("block")
    _results.append(_block)
    fields.append("timestamp")
    _results.append(_timestamp)

    final_result = {
        "block": block,
        "timestamp": _timestamp,
    }
    try:
        for i, result in enumerate(_results):
            # add only renderable fields
            if result and not issubclass(type(result), web3wrap):
                final_result[fields[i]] = result

        # return dict(zip(fields, _results))
    except Exception as e:
        raise e

    return final_result
