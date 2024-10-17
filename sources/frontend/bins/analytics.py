#
# Positions analysis / price vs Order Range
#
import asyncio
from decimal import Decimal
from sources.common.general.enums import Chain
from sources.common.prices.helpers import get_current_prices
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
    _project = {
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
    if ini_timestamp:
        _expr["$and"].append({"$gte": ["$timestamp", ini_timestamp]})
    if end_timestamp:
        _expr["$and"].append({"$lte": ["$timestamp", end_timestamp]})
    _query = [
        {"$project": _project},
        {"$match": {"$expr": _expr}},
        {"$sort": {"timestamp": 1}},
    ]

    # execute query
    _prices, _data, _latest_data = await asyncio.gather(
        get_current_prices(network=chain),
        local_database_helper(network=chain).get_items_from_database(
            collection_name="status", aggregate=_query
        ),
        local_database_helper(network=chain).get_items_from_database(
            collection_name="latest_hypervisor_snapshots",
            find={"address": hypervisor_address},
            projection=_project,
        ),
    )
    _prices = {itm["address"]: itm["price"] for itm in _prices}

    # add latest data to the list if there is any
    if _latest_data:
        # only one item is expected to be present in the latest collection
        _data.append(_latest_data[0])

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

        # convert tick to price: price ratio of token1 to token0, denoted as token1/token0
        itm["baseUpper"] = 1.0001 ** int(itm["baseUpper"]) / (
            10 ** itm["token1_decimals"] / 10 ** itm["token0_decimals"]
        )
        itm["baseLower"] = 1.0001 ** int(itm["baseLower"]) / (
            10 ** itm["token1_decimals"] / 10 ** itm["token0_decimals"]
        )
        itm["limitUpper"] = 1.0001 ** int(itm["limitUpper"]) / (
            10 ** itm["token1_decimals"] / 10 ** itm["token0_decimals"]
        )
        itm["limitLower"] = 1.0001 ** int(itm["limitLower"]) / (
            10 ** itm["token1_decimals"] / 10 ** itm["token0_decimals"]
        )
        itm["currentTick"] = 1.0001 ** int(itm["currentTick"]) / (
            10 ** itm["token1_decimals"] / 10 ** itm["token0_decimals"]
        )

        # outliers are zeroed out so that graph is not out of bounds
        if itm["currentTick"] > 1e7:
            itm["currentTick"] = 0
        # define a maximum bound for the graph, taking currentTick as reference from point
        _max_bound = 1e7
        for k in ["baseUpper", "baseLower", "limitUpper", "limitLower"]:
            if itm[k] > _max_bound:
                itm[k] = 0
            # elif itm[k] < -_max_bound:
            #     itm[k] = -_max_bound

        result.append(
            {
                "symbol": itm["symbol"],
                "timestamp": itm["timestamp"],
                "block": itm["block"],
                "currentTick": itm["currentTick"],
                "baseUpper": itm["baseUpper"],
                "baseLower": itm["baseLower"],
                "baseLiquidity_0": itm["baseLiquidity_0"],
                "baseLiquidity_1": itm["baseLiquidity_1"],
                "baseLiquidity_usd": itm["baseLiquidity_usd"],
                "limitUpper": itm["limitUpper"],
                "limitLower": itm["limitLower"],
                "limitLiquidity_0": itm["limitLiquidity_0"],
                "limitLiquidity_1": itm["limitLiquidity_1"],
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
    points_every: int | None = None,
) -> list[dict]:
    """Return a graph with the hypervisor returns

    Args:
        chain (Chain): _description_
        hypervisor_address (str): _description_
        ini_timestamp (int | None, optional): _description_. Defaults to None.
        end_timestamp (int | None, optional): _description_. Defaults to None.
        ini_block (int | None, optional): _description_. Defaults to None.
        end_block (int | None, optional): _description_. Defaults to None.
        points_every (int | None, optional): number of seconds between points. Defaults to None.

    Returns:
        list[dict]:
    """
    if hype_return_analysis := await build_hype_return_analysis_from_database(
        chain=chain,
        hypervisor_address=hypervisor_address,
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        ini_block=ini_block,
        end_block=end_block,
        use_latest_collection=True,
    ):
        return hype_return_analysis.get_graph(level="simple", points_every=points_every)

    return []


async def explain_hypervisor_returns(row1, row2) -> list[str]:
    """Create a LIST of strings to explain the graph returns of a hypervisor using two points of data representing a period

    Args:
        row1 (dict):
        row2 (dict):

    Returns:
        list[str]:
    """
    result = []
    # vars to use
    _days = (row2["timestamp"] - row1["timestamp"]) / (60 * 60 * 24)
    _price_token0 = (
        row2["status"]["end"]["prices"]["token0"]
        - row1["status"]["ini"]["prices"]["token0"]
    )
    _price_token1 = (
        row2["status"]["end"]["prices"]["token1"]
        - row1["status"]["ini"]["prices"]["token1"]
    )
    _share_price_ini = row1["status"]["ini"]["prices"]["share"]
    _share_price_end = (
        row2["status"]["end"]["prices"]["share"]
        + row2["rewards"]["period"]["per_share"]
    )
    _share_price_difference = _share_price_end - _share_price_ini
    _share_price_difference_percent = _share_price_difference / _share_price_ini
    ############ calc asset price move #############################################
    _share_price_asset_move = (
        _share_price_difference
        - row2["fees"]["period"]["per_share"]
        - row2["rewards"]["period"]["per_share"]
    )
    _share_price_asset_move_percent = _share_price_asset_move / _share_price_ini
    _share_price_fees_move = row2["fees"]["period"]["per_share"]
    _share_price_fees_move_percent = _share_price_fees_move / _share_price_ini
    _share_price_rewards_move = row2["rewards"]["period"]["per_share"]
    _share_price_rewards_move_percent = _share_price_rewards_move / _share_price_ini

    # during the X.X day period from xxx to xxx
    result.append(
        f" during the {_days:,.2f} day period from {row1['datetime_from']} to {row2['datetime_to']}"
    )
    result.append(
        f" {row1['chain']}'s {row1['symbol']} share price {'increased' if _share_price_difference>0 else 'decreased' if _share_price_difference<0 else 'stayed the same'} from ${_share_price_ini:,.2f} to ${_share_price_end:,.2f} [{_share_price_difference_percent:,.1%} ${_share_price_difference:,.2f}]"
    )
    # the price of token0 decreased X%
    result.append(
        f"      the price of token0 {'decreased' if _price_token0<0 else 'increased' if _price_token0>0 else 'stayed at'} {_price_token0/row1['status']['ini']['prices']['token0']:,.1%} [${_price_token0:,.2f}, from ${row1['status']['ini']['prices']['token0']:,.2f} to ${row2['status']['end']['prices']['token0']:,.2f}]"
    )
    # the price of token1 decreased X%
    result.append(
        f"      the price of token1 {'decreased' if _price_token1<0 else 'increased' if _price_token1>0 else 'stayed at'} {_price_token1/row1['status']['ini']['prices']['token1']:,.1%} [${_price_token1:,.2f}, from ${row1['status']['ini']['prices']['token1']:,.2f} to ${row2['status']['end']['prices']['token1']:,.2f}]"
    )
    ##############################################################
    # the underlying asset prices in USD moved the share price XXX
    result.append(
        f"      the underlying asset prices moved the share price {_share_price_asset_move_percent:,.1%} [${_share_price_asset_move:,.2f}]"
    )
    # the fees harvested by the position moved the share price XXX
    result.append(
        f"      the fees harvested by the position moved the share price {_share_price_fees_move_percent:,.1%} [${_share_price_fees_move:,.2f}]"
    )
    # the calculated rewards moved the share price XXX
    result.append(
        f"      the calculated rewards moved the share price {_share_price_rewards_move_percent:,.1%} [${_share_price_rewards_move:,.2f}]"
    )

    ### anualized data
    result.append(
        f"     the projected annual fee APR is {(_share_price_fees_move_percent/Decimal(str(_days)))*365:,.1%} [${(_share_price_fees_move/Decimal(str(_days)))*365:,.2f}]"
    )
    result.append(
        f"     the projected annual reward APR is {(_share_price_rewards_move_percent/Decimal(str(_days)))*365:,.1%} [${(_share_price_rewards_move/Decimal(str(_days)))*365:,.2f}]"
    )

    #
    return result
