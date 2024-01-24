#
# Positions analysis / price vs Order Range
#
import asyncio
from sources.common.general.enums import Chain
from sources.mongo.bins.apps.prices import get_current_prices
from sources.mongo.bins.apps.returns import build_hype_return_analysis_from_database
from sources.mongo.bins.helpers import local_database_helper


async def get_positions_analysis(
    chain: Chain,
    hypervisor_address: str,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
) -> list[dict]:
    # build query
    _expr = {
        "$and": [
            {"$eq": ["$address", hypervisor_address]},
        ]
    }
    if ini_timestamp:
        _expr["$and"].append({"$gte": ["$timestamp", ini_timestamp]})
    if end_timestamp:
        _expr["$and"].append({"$lte": ["$timestamp", end_timestamp]})
    _query = [
        {
            "$project": {
                "_id": 0,
                "address": "$address",
                "symbol": "$symbol",
                # "name": "$name",
                "timestamp": "$timestamp",
                "block": "$block",
                "currentTick": "$currentTick",
                "baseUpper": "$baseUpper",
                "baseLower": "$baseLower",
                "baseLiquidity_0": "$basePosition.amount0",
                "baseLiquidity_1": "$basePosition.amount1",
                "limitUpper": "$limitUpper",
                "limitLower": "$limitLower",
                "limitLiquidity_0": "$limitPosition.amount0",
                "limitLiquidity_1": "$limitPosition.amount1",
                "token0_symbol": "$pool.token0.symbol",
                "token1_symbol": "$pool.token1.symbol",
                "token0_address": "$pool.token0.address",
                "token1_address": "$pool.token1.address",
                "token0_decimals": "$pool.token0.decimals",
                "token1_decimals": "$pool.token1.decimals",
            }
        },
        {"$match": {"$expr": _expr}},
        {"$sort": {"timestamp": 1}},
    ]

    # execute query
    _prices, _data = await asyncio.gather(
        get_current_prices(network=chain),
        local_database_helper(network=chain).get_items_from_database(
            collection_name="status", aggregate=_query
        ),
    )
    _prices = {itm["address"]: itm["price"] for itm in _prices}

    # build result
    result = []
    for itm in _data:
        itm["baseLiquidity_0"] = (
            int(itm["baseLiquidity_0"]) / 10 ** itm["token0_decimals"]
        )
        itm["baseLiquidity_1"] = (
            int(itm["baseLiquidity_1"]) / 10 ** itm["token1_decimals"]
        )
        itm["limitLiquidity_0"] = (
            int(itm["limitLiquidity_0"]) / 10 ** itm["token0_decimals"]
        )
        itm["limitLiquidity_1"] = (
            int(itm["limitLiquidity_1"]) / 10 ** itm["token1_decimals"]
        )

        itm["baseLiquidity_usd"] = (
            itm["baseLiquidity_0"] * _prices[itm["token0_address"]]
        ) + (itm["baseLiquidity_1"] * _prices[itm["token1_address"]])
        itm["limitLiquidity_usd"] = (
            itm["limitLiquidity_0"] * _prices[itm["token0_address"]]
            + itm["limitLiquidity_1"] * _prices[itm["token1_address"]]
        )
        itm["totalLiquidity_usd"] = itm["baseLiquidity_usd"] + itm["limitLiquidity_usd"]

        result.append(
            {
                "symbol": itm["symbol"],
                "timestamp": itm["timestamp"],
                "block": itm["block"],
                "currentTick": int(itm["currentTick"]),
                "baseUpper": int(itm["baseUpper"]),
                "baseLower": int(itm["baseLower"]),
                # "baseLiquidity_0": itm["baseLiquidity_0"],
                # "baseLiquidity_1": itm["baseLiquidity_1"],
                "baseLiquidity_usd": itm["baseLiquidity_usd"],
                "limitUpper": int(itm["limitUpper"]),
                "limitLower": int(itm["limitLower"]),
                # "limitLiquidity_0": itm["limitLiquidity_0"],
                # "limitLiquidity_1": itm["limitLiquidity_1"],
                "limitLiquidity_usd": itm["limitLiquidity_usd"],
                # "totalLiquidity_usd": itm["totalLiquidity_usd"],
            }
        )

    return result


async def build_hypervisor_returns_graph(
    chain: Chain,
    hypervisor_address: str,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    ini_block: int | None = None,
    end_block: int | None = None,
):
    if hype_return_analysis := await build_hype_return_analysis_from_database(
        chain=chain,
        hypervisor_address=hypervisor_address,
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        ini_block=ini_block,
        end_block=end_block,
    ):
        return hype_return_analysis.get_graph(level="simple")
