import asyncio
from datetime import datetime, timezone
import logging

from fastapi import HTTPException


from sources.common.formulas.fees import convert_feeProtocol
from sources.internal.bins.internal import (
    InternalGrossFeesOutput,
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

    * When no timeframe is provided, it returns all available data.

    * The **usd** field is calculated using the current (now) price of the token.

    * **protocolFee_X** is the percentage of fees going to the protocol, from 1 to 100.

    * **collected fees** are the fees collected on rebalance and zeroBurn events.

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
                "timestamp": {"$lte": end_timestamp},
            }
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
        last_hypervisor_status[status["address"]] = status["last"]
        first_hypervisor_status[status["address"]] = status["first"]

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
        )

    return output


def calculate_pool_fees():
    pass


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
    """

    # prepare output structure
    output = {
        "hypervisors": 0,
        "deposits": 0,
        "withdraws": 0,
        "collectedFees": 0,
        "calculatedGrossFees": 0,
        "collectedFees_perDay": 0,
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

    return output
