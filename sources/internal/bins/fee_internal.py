import asyncio
from datetime import datetime, timezone
import logging

from fastapi import HTTPException


from sources.common.formulas.fees import convert_feeProtocol
from sources.common.general.enums import text_to_protocol
from sources.internal.bins.internal import (
    InternalGrossFeesOutput,
    InternalKpi,
    InternalTimeframe,
    InternalTokens,
)
from sources.mongo.bins.apps.prices import get_current_prices
from sources.mongo.bins.helpers import local_database_helper

from sources.common.database.collection_endpoint import database_local
from sources.common.database.common.collections_common import db_collections_common

from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import DEPLOYMENTS
from sources.web3.bins.w3.helpers import build_erc20_helper, build_hypervisor


async def get_fees(
    chain: Chain,
    protocol: Protocol | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict[str, InternalGrossFeesOutput]:
    """
        start hype status: database or on-the-fly snapshot
        end hype status: database or on-the-fly snapshot

          sum of all collected+zeroBurn fees for the database period
          sum of uncollected fees from end database hype status to end_timestamp/block


    Calculates the gross fees aquired (not uncollected) in a period of time for a specific protocol and chain using the protocol fee switch data.

    * When no timeframe is provided, it returns all available data.

    * The **usd** field is calculated using the current (now) price of the token.

    * **protocolFee_X** is the percentage of fees going to the protocol, from 1 to 100.

    * **collected fees** are the fees collected on rebalance and zeroBurn events.

    """

    if protocol and (protocol, chain) not in DEPLOYMENTS:
        raise HTTPException(
            status_code=400, detail=f"{protocol} on {chain} not available."
        )

    # create result dict
    output = {}

    # get hypervisors current prices
    token_prices = {x["address"]: x for x in await get_current_prices(network=chain)}
    # get all hypervisors last status from the database
    query_hype_status = [
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": "$address",
                "data": {"$first": "$$ROOT"},
            }
        },
    ]
    # create a match query part
    match = {}
    # filter by protocol
    if protocol:
        match["dex"] = protocol.database_name
    # filter by block or timestamp, when supplied
    if start_block and end_block:
        match["$and"] = [{"block": {"$gte": start_block}, "block": {"$lte": end_block}}]
    elif start_timestamp and end_timestamp:
        match["$and"] = [
            {
                "timestamp": {"$gte": start_timestamp},
                "timestamp": {"$lte": end_timestamp},
            }
        ]
    # add match to query
    if match:
        query_hype_status.insert(0, {"$match": match})

    last_hypervisor_status = {
        x["data"]["address"]: x["data"]
        for x in await local_database_helper(network=chain).get_items_from_database(
            collection_name="status",
            aggregate=query_hype_status,
        )
    }

    # set default period
    if not start_block and not start_timestamp:
        start_timestamp = int(datetime.now(timezone.utc).timestamp() - (86400 * 30))
    if not end_block and not end_timestamp:
        end_timestamp = int(datetime.now(timezone.utc).timestamp())

    # dummy hypervisor to get blockNumberFromTimestamp
    er2_dumm = build_erc20_helper(chain=chain)
    # calculate initial block from ini_timestamp
    start_block = start_block or await er2_dumm.blockNumberFromTimestamp(
        timestamp=start_timestamp
    )
    end_block = end_block or await er2_dumm.blockNumberFromTimestamp(
        timestamp=end_timestamp
    )

    # get a sumarized data portion for all hypervisors in the database for a period
    # when no period is specified, it will return all available data
    for hype_summary in await local_database_helper(
        network=chain
    ).query_items_from_database(
        collection_name="operations",
        query=database_local.query_operations_summary(
            hypervisor_addresses=list(last_hypervisor_status.keys()),
            timestamp_ini=start_timestamp,
            timestamp_end=end_timestamp,
            block_ini=start_block,
            block_end=end_block,
        ),
    ):
        # convert to float
        hype_summary = db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
        )

        try:
            hype_status_ini = build_hypervisor(
                network=chain,
                dex=protocol.database_name,
                block=start_block,
                hypervisor_address=hype_summary["address"],
            )
            ini_uncollected_fees = await hype_status_ini.get_fees_uncollected()
            hype_status_end = build_hypervisor(
                network=chain,
                dex=protocol.database_name,
                block=end_block,
                hypervisor_address=hype_summary["address"],
            )
            end_uncollected_fees = await hype_status_end.get_fees_uncollected()

            # ease hypervisor static data access
            hype_status = last_hypervisor_status.get(hype_summary["address"], {})
            if not hype_status:
                logging.getLogger(__name__).error(
                    f"Static data not found for hypervisor {hype_summary['address']}"
                )
                continue
            # ease hypervisor price access
            token0_price = token_prices.get(
                hype_status["pool"]["token0"]["address"], {}
            ).get("price", 0)
            token1_price = token_prices.get(
                hype_status["pool"]["token1"]["address"], {}
            ).get("price", 0)
            if not token0_price or not token1_price:
                logging.getLogger(__name__).error(
                    f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
                )
                continue

            # calculate protocol fees
            if "globalState" in hype_status["pool"]:
                protocol_fee_0_raw = hype_status["pool"]["globalState"][
                    "communityFeeToken0"
                ]
                protocol_fee_1_raw = hype_status["pool"]["globalState"][
                    "communityFeeToken1"
                ]
            else:
                # convert from 8 decimals
                protocol_fee_0_raw = hype_status["pool"]["slot0"]["feeProtocol"] % 16
                protocol_fee_1_raw = hype_status["pool"]["slot0"]["feeProtocol"] >> 4

            # convert to percent (0-100)
            protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
                feeProtocol0=protocol_fee_0_raw,
                feeProtocol1=protocol_fee_1_raw,
                hypervisor_protocol=hype_status["dex"],
                pool_protocol=hype_status["pool"]["dex"],
            )

            # calculate collected fees
            collectedFees_0 = (
                hype_summary["collectedFees_token0"]
                + hype_summary["zeroBurnFees_token0"]
                # remove uncollected initial fees and add uncollected final fees
                + float(str(end_uncollected_fees["qtty_token0"]))
                - float(str(ini_uncollected_fees["qtty_token0"]))
            )
            collectedFees_1 = (
                hype_summary["collectedFees_token1"]
                + hype_summary["zeroBurnFees_token1"]
                # remove uncollected initial fees and add uncollected final fees
                + float(str(end_uncollected_fees["qtty_token1"]))
                - float(str(ini_uncollected_fees["qtty_token1"]))
            )
            collectedFees_usd = (
                collectedFees_0 * token0_price + collectedFees_1 * token1_price
            )

            if protocol_fee_0 > 100 or protocol_fee_1 > 100:
                logging.getLogger(__name__).warning(
                    f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
                )

            # calculate gross fees
            if protocol_fee_0 < 100:
                grossFees_0 = collectedFees_0 / (1 - (protocol_fee_0 / 100))
            else:
                grossFees_0 = collectedFees_0

            if protocol_fee_1 < 100:
                grossFees_1 = collectedFees_1 / (1 - (protocol_fee_1 / 100))
            else:
                grossFees_1 = collectedFees_1

            grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

            # days period
            days_period = (
                hype_status_end._timestamp - hype_status_ini._timestamp
            ) / 86400

            # build output
            output[hype_summary["address"]] = InternalGrossFeesOutput(
                symbol=hype_status["symbol"],
                days_period=days_period,
                block=InternalTimeframe(
                    ini=hype_summary["block_ini"],
                    end=hype_summary["block_end"],
                ),
                timestamp=InternalTimeframe(
                    ini=hype_summary["timestamp_ini"],
                    end=hype_summary["timestamp_end"],
                ),
                deposits=InternalTokens(
                    token0=hype_summary["deposits_token0"],
                    token1=hype_summary["deposits_token1"],
                    usd=hype_summary["deposits_token0"] * token0_price
                    + hype_summary["deposits_token1"] * token1_price,
                ),
                withdraws=InternalTokens(
                    token0=hype_summary["withdraws_token0"],
                    token1=hype_summary["withdraws_token1"],
                    usd=hype_summary["withdraws_token0"] * token0_price
                    + hype_summary["withdraws_token1"] * token1_price,
                ),
                collectedFees=InternalTokens(
                    token0=collectedFees_0,
                    token1=collectedFees_1,
                    usd=collectedFees_usd,
                ),
                protocolFee_0=protocol_fee_0,
                protocolFee_1=protocol_fee_1,
                calculatedGrossFees=InternalTokens(
                    token0=grossFees_0,
                    token1=grossFees_1,
                    usd=grossFees_usd,
                ),
            )

        except Exception as e:
            logging.getLogger(__name__).error(
                f"Error calculating fees for hypervisor {hype_summary['address']}: {e}"
            )
            # add empty output as error
            # build output
            output[hype_summary["address"]] = InternalGrossFeesOutput(
                symbol="error",
                days_period=0,
            )

    return output


async def get_gross_fees(
    chain: Chain,
    protocol: Protocol | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict[str, InternalGrossFeesOutput]:
    """
    Calculates the gross fees aquired (not uncollected) in a period of time for a specific protocol and chain using the protocol fee switch data.
    """

    # create result dict
    output = {}

    # get hypervisors current prices
    token_prices = {x["address"]: x for x in await get_current_prices(network=chain)}
    # get all hypervisors last status from the database
    query_last_hype_status = [
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": "$address",
                "last": {"$first": "$$ROOT"},
                "first": {"$last": "$$ROOT"},
            }
        },
    ]
    # create a match query part
    match = {}
    # filter by protocol
    if protocol:
        match["dex"] = protocol.database_name
    # filter by block or timestamp, when supplied
    if start_block and end_block:
        match["$and"] = [{"block": {"$gte": start_block}, "block": {"$lte": end_block}}]
    elif start_timestamp and end_timestamp:
        match["$and"] = [
            {
                "timestamp": {"$gte": start_timestamp},
            },
            {
                "timestamp": {"$lte": end_timestamp},
            },
        ]
    # add match to query
    if match:
        query_last_hype_status.insert(0, {"$match": match})

    # last known hype status for each hypervisor at the end of the period
    last_hypervisor_status = {}
    first_hypervisor_status = {}
    for status in await local_database_helper(network=chain).get_items_from_database(
        collection_name="status",
        aggregate=query_last_hype_status,
    ):
        last_hypervisor_status[status["_id"]] = status["last"]
        first_hypervisor_status[status["_id"]] = status["first"]

    # when no hypes are found, return empty output
    if not last_hypervisor_status:
        return output

    # get a sumarized data portion for all hypervisors in the database for a period
    # when no period is specified, it will return all available data
    for hype_summary in await local_database_helper(
        network=chain
    ).query_items_from_database(
        collection_name="operations",
        query=database_local.query_operations_summary(
            hypervisor_addresses=list(last_hypervisor_status.keys()),
            timestamp_ini=start_timestamp,
            timestamp_end=end_timestamp,
            block_ini=start_block,
            block_end=end_block,
        ),
    ):
        # convert hype to float
        hype_summary = db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
        )

        # ease hypervisor status data access
        hype_status = last_hypervisor_status.get(hype_summary["address"], {})
        hype_status_ini = first_hypervisor_status.get(hype_summary["address"], {})
        if not hype_status:
            logging.getLogger(__name__).error(
                f"Last hype status data not found for {chain.fantasy_name}'s {protocol.fantasy_name} hypervisor {hype_summary['address']}"
            )
            continue
        # ease hypervisor price access
        token0_price = token_prices.get(
            hype_status["pool"]["token0"]["address"], {}
        ).get("price", 0)
        token1_price = token_prices.get(
            hype_status["pool"]["token1"]["address"], {}
        ).get("price", 0)
        if not token0_price or not token1_price:
            logging.getLogger(__name__).error(
                f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
            )
            continue

        # check for price outliers
        if token0_price > 1000000 or token1_price > 1000000:
            logging.getLogger(__name__).error(
                f" Price outlier detected for hypervisor {hype_summary['address']}: token0[{token0_price}] token1[{token1_price}]"
            )

        # calculate protocol fees
        if "globalState" in hype_status["pool"]:
            protocol_fee_0_raw = hype_status["pool"]["globalState"][
                "communityFeeToken0"
            ]
            protocol_fee_1_raw = hype_status["pool"]["globalState"][
                "communityFeeToken1"
            ]
        else:
            # convert from 8 decimals
            protocol_fee_0_raw = hype_status["pool"]["slot0"]["feeProtocol"] % 16
            protocol_fee_1_raw = hype_status["pool"]["slot0"]["feeProtocol"] >> 4

        # convert to percent (0-100)
        protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
            feeProtocol0=protocol_fee_0_raw,
            feeProtocol1=protocol_fee_1_raw,
            hypervisor_protocol=hype_status["dex"],
            pool_protocol=hype_status["pool"]["dex"],
        )

        # get pool fee tier
        pool_fee_tier = calculate_pool_fees(hype_status)

        # get gamma liquidity percentage
        gamma_liquidity_ini, gamma_liquidity_end = calculate_total_liquidity(
            hype_status_ini, hype_status
        )

        # calculate collected fees
        collectedFees_0 = (
            hype_summary["collectedFees_token0"] + hype_summary["zeroBurnFees_token0"]
        )
        collectedFees_1 = (
            hype_summary["collectedFees_token1"] + hype_summary["zeroBurnFees_token1"]
        )
        collectedFees_usd = (
            collectedFees_0 * token0_price + collectedFees_1 * token1_price
        )

        # uncollected fees at the last known database status
        try:
            uncollected_0 = float(hype_status["fees_uncollected"]["qtty_token0"]) / (
                10 ** hype_status["pool"]["token0"]["decimals"]
            )
            uncollected_1 = float(hype_status["fees_uncollected"]["qtty_token1"]) / (
                10 ** hype_status["pool"]["token1"]["decimals"]
            )
        except:
            uncollected_0 = 0
            uncollected_1 = 0

        if protocol_fee_0 > 100 or protocol_fee_1 > 100:
            logging.getLogger(__name__).warning(
                f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
            )

        # calculate gross fees
        if protocol_fee_0 < 100:
            grossFees_0 = collectedFees_0 / (1 - (protocol_fee_0 / 100))
        else:
            grossFees_0 = collectedFees_0

        if protocol_fee_1 < 100:
            grossFees_1 = collectedFees_1 / (1 - (protocol_fee_1 / 100))
        else:
            grossFees_1 = collectedFees_1

        grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

        # days period
        days_period = (
            hype_summary["timestamp_end"] - hype_summary["timestamp_ini"]
        ) / 86400

        # build output
        output[hype_summary["address"]] = InternalGrossFeesOutput(
            symbol=hype_status["symbol"],
            days_period=days_period,
            block=InternalTimeframe(
                ini=hype_summary["block_ini"],
                end=hype_summary["block_end"],
            ),
            timestamp=InternalTimeframe(
                ini=hype_summary["timestamp_ini"],
                end=hype_summary["timestamp_end"],
            ),
            deposits=InternalTokens(
                token0=hype_summary["deposits_token0"],
                token1=hype_summary["deposits_token1"],
                usd=hype_summary["deposits_token0"] * token0_price
                + hype_summary["deposits_token1"] * token1_price,
            ),
            withdraws=InternalTokens(
                token0=hype_summary["withdraws_token0"],
                token1=hype_summary["withdraws_token1"],
                usd=hype_summary["withdraws_token0"] * token0_price
                + hype_summary["withdraws_token1"] * token1_price,
            ),
            collectedFees=InternalTokens(
                token0=collectedFees_0,
                token1=collectedFees_1,
                usd=collectedFees_usd,
            ),
            uncollected=InternalTokens(
                token0=uncollected_0,
                token1=uncollected_1,
                usd=uncollected_0 * token0_price + uncollected_1 * token1_price,
            ),
            protocolFee_0=protocol_fee_0,
            protocolFee_1=protocol_fee_1,
            calculatedGrossFees=InternalTokens(
                token0=grossFees_0,
                token1=grossFees_1,
                usd=grossFees_usd,
            ),
            measurements=InternalKpi(
                gamma_vs_pool_liquidity_ini=gamma_liquidity_ini,
                gamma_vs_pool_liquidity_end=gamma_liquidity_end,
                feeTier=pool_fee_tier,
                eVolume=grossFees_usd / pool_fee_tier,
            ),
        )

    return output


def calculate_pool_fees(hypervisor_status: dict) -> float:
    """Calculate the fee charged by the pool on every swap

    Args:
        hypervisor_status (dict): hypervisor status

    Returns:
        float: percentage of fees the pool is charging
    """
    protocol = text_to_protocol(hypervisor_status["pool"]["dex"])
    fee_tier = 0

    if protocol == Protocol.CAMELOT:
        try:
            # Camelot:  (pool.globalState().feeZto + pool.globalState().feeOtz)/2
            fee_tier = (
                int(hypervisor_status["pool"]["globalState"]["feeZto"])
                + int(hypervisor_status["pool"]["globalState"]["feeOtz"])
            ) / 2
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")
    elif protocol == Protocol.RAMSES:
        # Ramses:  pool.currentFee()
        try:
            # 'currentFee' in here is actualy the 'fee' field
            fee_tier = int(hypervisor_status["pool"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")

    elif protocol == Protocol.QUICKSWAP:
        # QuickSwap + StellaSwap (Algebra V1):  pool.globalState().fee
        try:
            fee_tier = int(hypervisor_status["pool"]["globalState"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")
    else:
        # Uniswap: pool.fee()
        try:
            fee_tier = int(hypervisor_status["pool"]["fee"])
        except Exception as e:
            logging.getLogger(__name__).exception(f" {e}")

    return fee_tier / 1000000


def calculate_alwaysAquiringFeesPosition_max_pool_fees(
    hypervisor_status_ini: dict, hypervisor_status_end: dict, inDecimal: bool = False
) -> tuple[float, float]:
    """This will calculate the maximum fees collected in the period defined, for a position that is always aquiring fees.
        considerations:
            The position has been bound to the min-max tick the pool has been during the period
            The position's liquidity is fixed to the end position.

            Take into consideration that this is not the actual fees collected by the position but a theoretical view on a specific conditions.

    Args:
        hypervisor_status_ini (dict):
        hypervisor_status_end (dict):
        inDecimal (bool, optional): . Defaults to False.

    Returns:
        tuple[float, float]:
    """

    # feeGrowthGlobal period growth per liquidity
    feeGrowthGlobal0X128_growth = int(
        hypervisor_status_end["pool"]["feeGrowthGlobal0X128"]
    ) - int(hypervisor_status_ini["pool"]["feeGrowthGlobal0X128"])
    feeGrowthGlobal1X128_growth = int(
        hypervisor_status_end["pool"]["feeGrowthGlobal1X128"]
    ) - int(hypervisor_status_ini["pool"]["feeGrowthGlobal1X128"])

    # position liquidity at the end of the period
    # liquidity_ini = calculate_inRange_liquidity(hypervisor_status_ini)
    liquidity_end = calculate_inRange_liquidity(hypervisor_status_end)

    # pool_liquidity_variation = int(hypervisor_status_end["pool"]["liquidity"]) - int(
    #     hypervisor_status_ini["pool"]["liquidity"]
    # )

    # maximum fees that can be possibly collected in the period by an always aquiring fee position
    max_fees_0 = (liquidity_end * feeGrowthGlobal0X128_growth) // (2**128)
    max_fees_1 = (liquidity_end * feeGrowthGlobal1X128_growth) // (2**128)

    if inDecimal:
        max_fees_0 /= 10 ** hypervisor_status_end["pool"]["token0"]["decimals"]
        max_fees_1 /= 10 ** hypervisor_status_end["pool"]["token1"]["decimals"]

    return max_fees_0, max_fees_1


def calculate_total_liquidity(
    hypervisor_status_ini: dict, hypervisor_status_end: dict
) -> tuple[float, float]:
    """Percentage of liquidity gamma has in the pool

    Args:
        hypervisor_status_ini (dict):  initial hypervisor status
        hypervisor_status_end (dict):  end hypervisor status

    Returns:
        tuple[float,float]:  initial_percentage, end_percentage
    """

    liquidity_ini = calculate_inRange_liquidity(hypervisor_status_ini)
    liquidity_end = calculate_inRange_liquidity(hypervisor_status_end)

    initial_percentage = (
        liquidity_ini / int(hypervisor_status_ini["pool"]["liquidity"])
        if int(hypervisor_status_ini["pool"]["liquidity"])
        else 0
    )
    end_percentage = (
        liquidity_end / int(hypervisor_status_end["pool"]["liquidity"])
        if int(hypervisor_status_end["pool"]["liquidity"])
        else 0
    )

    if end_percentage > 1:
        logging.getLogger(__name__).warning(
            f" liquidity percentage > 1 on {hypervisor_status_end['dex']}  {hypervisor_status_end['address']} hype block {hypervisor_status_end['block']}"
        )
    if initial_percentage > 1:
        logging.getLogger(__name__).warning(
            f" liquidity percentage > 1 on {hypervisor_status_ini['dex']}  {hypervisor_status_ini['address']} hype block {hypervisor_status_ini['block']}"
        )
    return initial_percentage, end_percentage


def calculate_inRange_liquidity(hypervisor_status: dict) -> int:
    """Calculate the liquidity in range of a hypervisor

    Args:
        hypervisor_status (dict):  hypervisor status

    Returns:
        int: liquidity in range
    """

    current_tick = (
        int(hypervisor_status["pool"]["slot0"]["tick"])
        if "slot0" in hypervisor_status["pool"]
        else int(hypervisor_status["pool"]["globalState"]["tick"])
    )

    liquidity = 0
    # check what to add as liquidity ( inRange only )
    if (
        float(hypervisor_status["limitUpper"]) >= current_tick
        and float(hypervisor_status["limitLower"]) <= current_tick
    ):
        liquidity += int(hypervisor_status["limitPosition"]["liquidity"])
    if (
        float(hypervisor_status["baseUpper"]) >= current_tick
        and float(hypervisor_status["baseLower"]) <= current_tick
    ):
        liquidity += int(hypervisor_status["basePosition"]["liquidity"])

    return liquidity


async def get_chain_usd_fees(
    chain: Chain,
    protocol: Protocol | None = None,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
    weeknum: int | None = None,
) -> dict:
    """
    Returns the total current priced USD fees collected in a period of time for a specific chain

    * When no timeframe is provided, it returns all available data.

    * The **usd** field is calculated using the current (now) price of the token.

    * **collected fees** are the fees collected on rebalance and zeroBurn events.

    * **collectedFees_perDay** are the daily fees collected in the period.

    * **eVolume** is the estimated volume of the pool in the period, calculated using the collectedFees_perDay and the feeTier.
    """

    # prepare output structure
    output = {
        "hypervisors": 0,
        "deposits": 0,
        "withdraws": 0,
        "collectedFees": 0,
        "calculatedGrossFees": 0,
        "collectedFees_perDay": 0,
        "eVolume": 0,
    }

    if start_block and end_block:
        output["block"] = InternalTimeframe(ini=start_block, end=end_block)
    if start_timestamp and end_timestamp:
        output["timestamp"] = InternalTimeframe(ini=start_timestamp, end=end_timestamp)

    # add supplied weeknum to output, when supplied
    if weeknum:
        output["weeknum"] = weeknum

    data = await get_gross_fees(
        chain=chain,
        protocol=protocol,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        start_block=start_block,
        end_block=end_block,
    )
    for hypervisor_address, hypervisor_data in data.items():
        # discard outliers
        if hypervisor_data.calculatedGrossFees.usd > 10**10:
            logging.getLogger(__name__).warning(
                f" {hypervisor_address} has an outlier calculatedGrossFees.usd of {hypervisor_data.calculatedGrossFees.usd}"
            )
            # continue
        output["hypervisors"] += 1
        output["deposits"] += hypervisor_data.deposits.usd
        output["withdraws"] += hypervisor_data.withdraws.usd
        output["collectedFees"] += hypervisor_data.collectedFees.usd
        output["calculatedGrossFees"] += hypervisor_data.calculatedGrossFees.usd
        output["collectedFees_perDay"] += (
            (hypervisor_data.collectedFees.usd / hypervisor_data.days_period)
            if hypervisor_data.days_period
            else 0
        )
        output["eVolume"] += hypervisor_data.measurements.eVolume

    return output


# frontend


async def get_revenue_operations(
    chain: Chain,
    protocol: Protocol,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    yearly: bool = False,
) -> list:
    """Return the revenue operations for a specific protocol and chain by month and year, if requested.

    Args:
        chain (Chain):
        protocol (Protocol):
        ini_timestamp (int | None, optional): limit data from  . Defaults to None.
        end_timestamp (int | None, optional): limit data to . Defaults to None.
        yearly (bool, optional): group results by year. Defaults to False.

    Returns:
        list:
    """

    # build first match
    _match = {
        "dex": {"$exists": True},
        "$or": [
            {"src": {"$ne": "0x0000000000000000000000000000000000000000"}},
            {"user": {"$exists": True}},
        ],
    }
    # apply filter s
    if protocol:
        _match["dex"] = protocol.database_name
    if ini_timestamp and end_timestamp:
        _match["$and"] = [
            {"timestamp": {"$gte": ini_timestamp}},
            {"timestamp": {"$lte": end_timestamp}},
        ]
    elif ini_timestamp:
        _match["timestamp"] = {"$gte": ini_timestamp}
    elif end_timestamp:
        _match["timestamp"] = {"$lte": end_timestamp}

    # build query
    _query = [
        {"$match": _match},
        {
            "$project": {
                "_id": 0,
                "year": "$year",
                "month": "$month",
                "dex": "$dex",
                "hypervisor": {"$cond": ["$user", 0, "$src"]},
                "token": {"$cond": ["$token", "$token", "$address"]},
                "token_symbol": "$symbol",
                "timestamp": "$timestamp",
                "usd_value": "$usd_value",
            }
        },
        {
            "$group": {
                "_id": {
                    "year": "$year",
                    "month": "$month",
                    "token": "$address",
                    "hypervisor": "$hypervisor",
                    "dex": "$dex",
                },
                "token_symbol": {"$first": "$token_symbol"},
                "total_usd": {"$sum": "$usd_value"},
            }
        },
        {
            "$lookup": {
                "from": "static",
                "let": {"op_address": "$_id.hypervisor"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "address": "$address",
                            "symbol": "$symbol",
                            "pool": {
                                "address": "$pool.address",
                                "token0": "$pool.token0.address",
                                "token1": "$pool.token1.address",
                                "dex": "$pool.dex",
                            },
                            "dex": "$dex",
                        }
                    },
                    {"$unset": ["_id"]},
                ],
                "as": "static",
            }
        },
        {"$unwind": {"path": "$static", "preserveNullAndEmptyArrays": True}},
        {"$sort": {"_id.year": 1, "_id.month": 1, "total_usd": -1}},
        {
            "$group": {
                "_id": {
                    "year": "$_id.year",
                    "month": "$_id.month",
                    "dex": "$_id.dex",
                    "hypervisor_symbol": "$static.symbol",
                },
                "total_usd": {"$sum": "$total_usd"},
                "items": {
                    "$push": {
                        "token": "$_id.token",
                        "hypervisor": "$_id.hypervisor",
                        "token_symbol": "$token_symbol",
                        "total_usd": "$total_usd",
                        "pool": "$static.pool.address",
                    }
                },
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1, "total_usd": -1}},
        {
            "$group": {
                "_id": {"year": "$_id.year", "month": "$_id.month", "dex": "$_id.dex"},
                "total_usd": {"$sum": "$total_usd"},
                "items": {
                    "$push": {
                        "hypervisor_symbol": "$_id.hypervisor_symbol",
                        "total_usd": "$total_usd",
                        "items": "$items",
                    }
                },
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1, "total_usd": -1}},
        {
            "$group": {
                "_id": {"year": "$_id.year", "month": "$_id.month"},
                "total_usd": {"$sum": "$total_usd"},
                "items": {
                    "$push": {
                        "dex": "$_id.dex",
                        "total_usd": "$total_usd",
                        "items": "$items",
                    }
                },
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1}},
        {
            "$project": {
                "_id": 0,
                "year": "$_id.year",
                "month": "$_id.month",
                "total_usd": "$total_usd",
                "items": "$items",
            }
        },
    ]

    # group by year, if requested
    if yearly:
        _query.append(
            {
                "$group": {
                    "_id": "$year",
                    "total_usd": {"$sum": "$total_usd"},
                    "items": {
                        "$push": {
                            "month": "$month",
                            "total_usd": "$total_usd",
                            "items": "$items",
                        }
                    },
                }
            }
        )
    return await local_database_helper(network=chain).query_items_from_database(
        collection_name="revenue_operations", query=_query
    )
