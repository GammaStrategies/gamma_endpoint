import asyncio
from datetime import datetime, timezone
import logging
import time

from fastapi import HTTPException


from sources.common.formulas.fees import convert_feeProtocol
from sources.common.general.enums import text_to_protocol
from sources.common.prices.helpers import (
    get_current_prices,
    get_database_prices_closeto,
)
from sources.internal.bins.fee_internal import get_gross_fees
from sources.internal.bins.internal import (
    InternalGrossFeesOutput,
    InternalKpi,
    InternalTimeframe,
    InternalTokens,
)
from sources.mongo.bins.helpers import local_database_helper

from sources.common.database.collection_endpoint import database_local

from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import DEPLOYMENTS


async def get_average_tvl(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
    prices: dict[str, dict] | None = None,
) -> dict:
    # define chains
    if chain:
        chains = [chain]
    else:
        chains = list(Chain)

    # build hypervisor data query
    _query = _query_average_tvl_static(
        protocol=protocol,
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        hypervisors=hypervisors,
    )
    # build output
    output = {
        "ini_timestamp": ini_timestamp,
        "end_timestamp": end_timestamp,
        "average_tvl": 0,
        "chains": {},  #  "chain": "" "average_tvl": 0, "hypervisors": []},
    }

    _start_time = time.time()
    # execute tasks
    for chain in chains:
        chain_output = {
            "chain_id": chain.id,
            "chain": chain.fantasy_name,
            "average_tvl": 0,
            "hypervisors": [],
        }
        if prices is None:
            # get current prices and hypervisor data from database
            _prices, _data = await asyncio.gather(
                get_database_prices_closeto(
                    chain=chain,
                    timestamp=end_timestamp,
                    default_to_current=True,
                ),
                # get_current_prices(network=chain),
                local_database_helper(network=chain).get_items_from_database(
                    collection_name="static", aggregate=_query
                ),
            )
        else:
            _data = await local_database_helper(network=chain).get_items_from_database(
                collection_name="static", aggregate=_query
            )
            _prices = prices

        for itm in _data:
            # convert Decimal128 to float
            itm = database_local.convert_decimal_to_float(
                database_local.convert_d128_to_decimal(itm)
            )
            try:
                # ease to use variables
                _price0 = _prices[itm["token0"]]["price"]
                _price1 = _prices[itm["token1"]]["price"]
                # calculate tvls
                av_tvl = (
                    itm["tvl"]["av_tvl0"] * _price0 + itm["tvl"]["av_tvl1"] * _price1
                )
                max_tvl = (
                    itm["tvl"]["max_tvl0"] * _price0 + itm["tvl"]["max_tvl1"] * _price1
                )
                min_tvl = (
                    itm["tvl"]["min_tvl0"] * _price0 + itm["tvl"]["min_tvl1"] * _price1
                )
            except KeyError:
                _price0 = av_tvl = max_tvl = min_tvl = 0
            except Exception as e:
                logging.getLogger(__name__).error(f" Error  {e}")

            # add to global output
            output["average_tvl"] += av_tvl
            # add to chain output
            chain_output["average_tvl"] += av_tvl
            chain_output["hypervisors"].append(
                {
                    "address": itm["address"],
                    "symbol": itm["symbol"],
                    "av_tvl": av_tvl,
                    "max_tvl": max_tvl,
                    "min_tvl": min_tvl,
                }
            )

        # add to global output
        output["chains"][chain.id] = chain_output

    #
    logging.getLogger(__name__).info(
        f" get_average_tvl took {time.time()-_start_time} seconds."
    )
    return output


async def get_transactions(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
    prices: dict[str, dict] | None = None,
) -> dict:
    # define chains
    if chain:
        chains = [chain]
    else:
        chains = list(Chain)

    # build hypervisor data query
    _query = _query_transactions_operations(
        protocol=protocol,
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        hypervisors=hypervisors,
    )

    days = (
        (end_timestamp or datetime.now(timezone.utc).timestamp()) - ini_timestamp
    ) / 86400

    # build output
    output = {
        "ini_timestamp": ini_timestamp,
        "end_timestamp": end_timestamp,
        "days_period": days,
    }  #  <chain>:{<transaction type>: { } }

    # execute tasks
    for chain in chains:
        chain_output = {
            "chain_id": chain.id,
            "chain": chain.fantasy_name,
            "tvl_variation_usd": 0,
            "withdraws_usd": 0,
            "withdraws_qtty": 0,
            "deposits_usd": 0,
            "deposits_qtty": 0,
            "transfers_qtty": 0,
            "approvals_qtty": 0,
            "zeroBurns_usd": 0,
            "zeroBurns_qtty": 0,
            "rebalances_usd": 0,
            "rebalances_qtty": 0,
            "withdraws_shares": 0,
            "deposits_shares": 0,
            "hypervisors": [],
        }

        if prices is None:
            # get current prices and hypervisor data from database
            _prices, _data = await asyncio.gather(
                get_database_prices_closeto(
                    chain=chain,
                    timestamp=end_timestamp,
                    default_to_current=True,
                ),
                # get_current_prices(network=chain),
                local_database_helper(network=chain).get_items_from_database(
                    collection_name="static", aggregate=_query
                ),
            )
        else:
            _data = await local_database_helper(network=chain).get_items_from_database(
                collection_name="static", aggregate=_query
            )
            _prices = prices

        for itm in _data:
            hypervisor_output = {
                "address": itm["address"],
                "symbol": itm["symbol"],
                "tvl_variation_usd": 0,
                "withdraws_usd": 0,
                "withdraws_qtty": 0,
                "deposits_usd": 0,
                "deposits_qtty": 0,
                "transfers_qtty": 0,
                "approvals_qtty": 0,
                "zeroBurns_usd": 0,
                "zeroBurns_qtty": 0,
                "rebalances_usd": 0,
                "rebalances_qtty": 0,
                "withdraws_shares": 0,
                "deposits_shares": 0,
                "hypervisors": [],
            }

            # convert Decimal128 to float
            itm = database_local.convert_decimal_to_float(
                database_local.convert_d128_to_decimal(itm)
            )

            try:
                # ease to use variables
                _price0 = _prices[itm["token0"]]["price"]
                _price1 = _prices[itm["token1"]]["price"]

                for operation in itm["operations"]:
                    _usd = (
                        operation["qtty_token0"] * _price0
                        + operation["qtty_token1"] * _price1
                    )

                    if operation["_id"] == "withdraw":
                        hypervisor_output["tvl_variation_usd"] -= _usd
                        hypervisor_output["withdraws_usd"] += _usd
                        hypervisor_output["withdraws_qtty"] += operation["counter"]
                        hypervisor_output["withdraws_shares"] += operation["shares"]
                    elif operation["_id"] == "deposit":
                        hypervisor_output["tvl_variation_usd"] += _usd
                        hypervisor_output["deposits_usd"] += _usd
                        hypervisor_output["deposits_qtty"] += operation["counter"]
                        hypervisor_output["deposits_shares"] += operation["shares"]
                    elif operation["_id"] == "approve":
                        hypervisor_output["approvals_qtty"] += operation["counter"]
                    elif operation["_id"] == "zeroBurn":
                        hypervisor_output["tvl_variation_usd"] += _usd
                        hypervisor_output["zeroBurns_usd"] += _usd
                        hypervisor_output["zeroBurns_qtty"] += operation["counter"]
                    elif operation["_id"] == "rebalance":
                        hypervisor_output["tvl_variation_usd"] += _usd
                        hypervisor_output["rebalances_usd"] += _usd
                        hypervisor_output["rebalances_qtty"] += operation["counter"]
                    elif operation["_id"] == "transfer":
                        hypervisor_output["transfers_qtty"] += operation["counter"]
            except KeyError:
                pass

            # add to chain output
            chain_output["hypervisors"].append(hypervisor_output)
            for k, v in hypervisor_output.items():
                if k not in ["address", "symbol"]:
                    chain_output[k] += v

            # sort hypes by total output
            chain_output["hypervisors"] = sorted(
                chain_output["hypervisors"],
                key=lambda x: x["withdraws_usd"]
                + x["deposits_usd"]
                + x["deposits_shares"]
                + x["withdraws_shares"]
                + x["rebalances_usd"]
                + x["zeroBurns_usd"],
                reverse=True,
            )

            # add to total output
            output[chain.id] = chain_output

    #
    return output


async def get_transactions_summary(
    chain: Chain,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
    prices: dict[str, dict] | None = None,
):
    output = {
        "ini_timestamp": ini_timestamp,
        "end_timestamp": end_timestamp,
        "days_period": (
            (end_timestamp or datetime.now(timezone.utc).timestamp()) - ini_timestamp
        )
        / 86400,
        "tvl_variation_usd": 0,
        "new_users_usd": 0,
        "fees_usd": 0,
        "gross_fees_usd": 0,
        "volume": 0,
        "details": {},
    }

    # query_operations_summary
    gFess = await get_gross_fees(
        chain=chain,
        protocol=protocol,
        start_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        hypervisor_addresses=hypervisors,
        prices=prices,
    )

    for hype_address, hype_data in gFess.items():
        new_users_usd = hype_data.deposits.usd - hype_data.withdraws.usd
        fees_usd = hype_data.collectedFees.usd + hype_data.uncollected.usd
        tvl_variation_usd = new_users_usd + fees_usd

        output["tvl_variation_usd"] += tvl_variation_usd
        output["new_users_usd"] += new_users_usd
        output["fees_usd"] += fees_usd
        output["gross_fees_usd"] += hype_data.calculatedGrossFees.usd
        output["volume"] += hype_data.measurements.eVolume
        output["details"][hype_address] = hype_data

    return output


async def get_users_activity(
    chain: Chain | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
) -> dict:
    """User activity is defined by deposits and withdraws.

    Args:
        chain (Chain | None, optional): . Defaults to None.
        ini_timestamp (int | None, optional): . Defaults to None.
        end_timestamp (int | None, optional): . Defaults to None.
        hypervisors (list[str] | None, optional): list of hypervisor addresses to filter. Defaults to None.

    Returns:
        dict: { "total_users": int, "addresses": { "address": int, ... } }
    """
    _query = _query_users_activity(
        ini_timestamp=ini_timestamp,
        end_timestamp=end_timestamp,
        hypervisors=hypervisors,
    )
    chains = [chain] if chain else list(set([cha for pro, cha in DEPLOYMENTS]))

    output = {"total_users": 0, "addresses": {}}
    for chain_result in await asyncio.gather(
        *[
            local_database_helper(network=chain).get_items_from_database(
                collection_name="operations", aggregate=_query
            )
            for chain in chains
        ]
    ):
        for user in chain_result:
            if user["user"] not in output["addresses"]:
                output["addresses"][user["user"]] = user["activity_count"]
                output["total_users"] += 1
            else:
                output["addresses"][user["user"]] += user["activity_count"]

    return output


# HELPERS


# gathered from hypervisor's totalAmounts
def _query_average_tvl_static(
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
) -> list[dict]:
    """Database query to get average tvl, gathere from hypervisor's totalAmounts.
    Args:
        protocol (Protocol | None, optional): . Defaults to all protocols.
        ini_timestamp (int | None, optional): . Defaults to all time.
        end_timestamp (int | None, optional): . Defaults to all time.

    Returns:
        list[dict]: executing this query will return a list of
        {  "address": str,
              "symbol": str,
              "tvl": {
                "av_tvl0": float,
                "av_tvl1": float,
                "max_tvl0": float,
                "max_tvl1": float,
                "min_tvl0": float,
                "min_tvl1": float,

        }
    """
    # build status filter:
    _status_and_expr = {
        "$and": [
            {"$eq": ["$address", "$$op_address"]},
        ]
    }
    if ini_timestamp:
        _status_and_expr["$and"].append({"$gte": ["$timestamp", ini_timestamp]})
    if end_timestamp:
        _status_and_expr["$and"].append({"$lte": ["$timestamp", end_timestamp]})

    query = [
        {
            "$project": {
                "address": "$address",
                "symbol": "$symbol",
                "token0": "$pool.token0.address",
                "token1": "$pool.token1.address",
            }
        },
        {
            "$lookup": {
                "from": "status",
                "let": {"op_address": "$address"},
                "pipeline": [
                    {
                        "$project": {
                            "address": "$address",
                            "timestamp": "$timestamp",
                            "tvl0": {
                                "$divide": [
                                    {"$toDecimal": "$totalAmounts.total0"},
                                    {"$pow": [10, "$pool.token0.decimals"]},
                                ]
                            },
                            "tvl1": {
                                "$divide": [
                                    {"$toDecimal": "$totalAmounts.total1"},
                                    {"$pow": [10, "$pool.token1.decimals"]},
                                ]
                            },
                        }
                    },
                    {"$match": {"$expr": _status_and_expr}},
                    {
                        "$group": {
                            "_id": 0,
                            "av_tvl0": {"$avg": "$tvl0"},
                            "av_tvl1": {"$avg": "$tvl1"},
                            "max_tvl0": {"$max": "$tvl0"},
                            "max_tvl1": {"$max": "$tvl1"},
                            "min_tvl0": {"$min": "$tvl0"},
                            "min_tvl1": {"$min": "$tvl1"},
                        }
                    },
                ],
                "as": "tvl",
            }
        },
        {"$unset": ["_id"]},
        {"$unwind": "$tvl"},
    ]

    # static filters
    if hypervisors:
        query.insert(0, {"$match": {"address": {"$in": hypervisors}}})
    elif protocol:
        query.insert(0, {"$match": {"dex": protocol.database_name}})

    return query


def _query_transactions_operations(
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
) -> list[dict]:
    _operations_and_expr = {
        "$and": [
            {"$eq": ["$address", "$$op_address"]},
        ]
    }
    if ini_timestamp:
        _operations_and_expr["$and"].append({"$gte": ["$timestamp", ini_timestamp]})
    if end_timestamp:
        _operations_and_expr["$and"].append({"$lte": ["$timestamp", end_timestamp]})
    # { "$or": [{"src":"$$op_address"},{"dst":"$$op_address"}]},
    # { "$in": ["$topic",["deposit","withdraw"]]},
    query = [
        {
            "$project": {
                "address": "$address",
                "symbol": "$symbol",
                "token0": "$pool.token0.address",
                "token1": "$pool.token1.address",
            }
        },
        {
            "$lookup": {
                "from": "operations",
                "let": {"op_address": "$address"},
                "pipeline": [
                    {"$match": {"$expr": _operations_and_expr}},
                    {
                        "$group": {
                            "_id": "$topic",
                            "qtty_token0": {
                                "$sum": {
                                    "$divide": [
                                        {"$toDecimal": "$qtty_token0"},
                                        {"$pow": [10, "$decimals_token0"]},
                                    ]
                                }
                            },
                            "qtty_token1": {
                                "$sum": {
                                    "$divide": [
                                        {"$toDecimal": "$qtty_token1"},
                                        {"$pow": [10, "$decimals_token1"]},
                                    ]
                                }
                            },
                            "shares": {
                                "$sum": {
                                    "$divide": [
                                        {"$toDecimal": "$shares"},
                                        {"$pow": [10, "$decimals_contract"]},
                                    ]
                                }
                            },
                            "counter": {"$sum": 1},
                        }
                    },
                ],
                "as": "operations",
            }
        },
    ]

    # add static filters
    if hypervisors:
        # lower case hype addresses to match database
        hypervisors = [hype.lower() for hype in hypervisors]
        query.insert(0, {"$match": {"address": {"$in": hypervisors}}})
    elif protocol:
        query.insert(0, {"$match": {"dex": protocol.database_name}})

    return query


def _query_users_activity(
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    hypervisors: list[str] | None = None,
) -> list[dict]:
    """Using deposits and withdraws to define user activity.

    Args:
        ini_timestamp (int | None, optional): . Defaults to None.
        end_timestamp (int | None, optional): . Defaults to None.
        hypervisors (list[str] | None, optional): addresses. Defaults to None.

    Returns:
        list[dict]: query to get user activity
    """
    # build match
    _match = {"topic": {"$in": ["deposit", "withdraw"]}}
    # add and filters
    _and = []
    if ini_timestamp:
        _and.append({"timestamp": {"$gte": ini_timestamp}})
    if end_timestamp:
        _and.append({"timestamp": {"$lte": end_timestamp}})
    if hypervisors:
        _and.append({"address": {"$in": hypervisors}})

    if _and:
        _match["$and"] = _and

    # build query
    return [
        {"$match": _match},
        {
            "$addFields": {
                "user": {"$cond": [{"$eq": ["$topic", "deposit"]}, "$to", "$sender"]}
            }
        },
        {"$group": {"_id": "$user", "activity_count": {"$sum": 1}}},
        {"$project": {"_id": 0, "user": "$_id", "activity_count": "$activity_count"}},
    ]
