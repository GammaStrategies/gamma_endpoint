import asyncio
from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import database_global, database_local

from sources.common.database.common.collections_common import db_collections_common

from sources.web3.bins.database.db_raw_direct_info import direct_db_hypervisor_info

from ..helpers import local_database_helper, global_database_helper


# Hypervisors


async def hypervisors_list(network: Chain, protocol: Protocol):
    return await local_database_helper(network=network).get_items_from_database(
        collection_name="static",
        find={"dex": protocol.database_name},
        projection={"_id": 0},
    )


async def hypervisors_last_snapshot(
    network: Chain,
    protocol: Protocol | None = None,
    hypervisor_address: str | None = None,
) -> list[dict]:
    """Get the last snapshot for all hypervisors and their rewards.

    This function retrieves the last snapshot for all hypervisors and their rewards.
    It uses the latest_hypervisor_snapshots and latest_reward_snapshots collections.

    Args:
        network (Chain): The network to retrieve the snapshots from.
        protocol (Protocol | None, optional): The protocol to filter the snapshots by. Defaults to None.
        hypervisor_address (str | None, optional): The address of the hypervisor to filter the snapshots by. Defaults to None.

    Returns:
        List[dict]: A list of dictionaries representing the last snapshot for each hypervisor and their rewards.
    """

    _match = {}
    _query = [
        {"$project": {"_id": 0, "id": 0, "rpc_costs": 0, "fees_collected": 0}},
        {
            "$lookup": {
                "from": "latest_reward_snapshots",
                "let": {"op_address": "$address"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$hypervisor_address", "$$op_address"]}
                                ]
                            }
                        }
                    },
                    {"$project": {"_id": 0, "id": 0, "rpc_costs": 0}},
                ],
                "as": "rewards",
            }
        },
    ]

    if hypervisor_address:
        _match["address"] = hypervisor_address.lower()
    elif protocol:
        _match["dex"] = protocol.database_name

    if _match:
        _query.insert(0, {"$match": _match})

    return await local_database_helper(network=network).get_items_from_database(
        aggregate=_query,
        collection_name="latest_hypervisor_snapshots",
    )


async def hypervisors_uncollected_fees(
    network: Chain,
    timestamp: int | None = None,
    block: int | None = None,
) -> dict:
    """Get the uncollected fees for all hypervisors."""
    return [
        db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=item)
        )
        for item in await local_database_helper(
            network=network
        ).query_items_from_database(
            collection_name="status",
            query=database_local.query_uncollected_fees(
                timestamp=timestamp, block=block
            ),
        )
    ]


async def hypervisors_collected_fees(
    network: Chain,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict:
    """Get the collected fees for a hypervisor."""
    # convert decimals to float
    return [
        db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=item)
        )
        for item in await local_database_helper(
            network=network
        ).query_items_from_database(
            collection_name="operations",
            query=database_local.query_operations_summary(
                timestamp_ini=start_timestamp,
                timestamp_end=end_timestamp,
                block_ini=start_block,
                block_end=end_block,
            ),
        )
    ]


async def hypervisors_rewards_status(network: Chain, protocol: Protocol):
    """Get the rewards status for all hypervisors."""
    query = [
        {"$match": {"dex": protocol.database_name}},
        {"$sort": {"block": -1}},
        {
            "$group": {
                "_id": {
                    "hypervisor_address": "$hypervisor_address",
                    "rewardToken": "$rewardToken",
                },
                "data": {"$first": "$$ROOT"},
            },
        },
        {"$sort": {"hypervisor_address": 1}},
        {"$replaceRoot": {"newRoot": "$data"}},
        {"$unset": ["_id", "id"]},
    ]
    return await local_database_helper(network=network).get_items_from_database(
        collection_name="rewards_status",
        aggregate=query,
    )


# Hypervisor


async def hypervisor_uncollected_fees(
    network: Chain,
    hypervisor_address: str,
    timestamp: int | None = None,
    block: int | None = None,
) -> dict:
    """Get the uncollected fees for a hypervisor."""
    return [
        db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=item)
        )
        for item in await local_database_helper(
            network=network
        ).query_items_from_database(
            collection_name="status",
            query=database_local.query_uncollected_fees(
                hypervisor_address=hypervisor_address, timestamp=timestamp, block=block
            ),
        )
    ]


async def hypervisor_collected_fees(
    network: Chain,
    hypervisor_address: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict:
    """Get the collected fees for a hypervisor."""

    return [
        db_collections_common.convert_decimal_to_float(
            item=db_collections_common.convert_d128_to_decimal(item=item)
        )
        for item in await local_database_helper(
            network=network
        ).query_items_from_database(
            collection_name="operations",
            query=database_local.query_operations_summary(
                hypervisor_address=[hypervisor_address],
                timestamp_ini=start_timestamp,
                timestamp_end=end_timestamp,
                block_ini=start_block,
                block_end=end_block,
            ),
        )
    ]


async def get_hypervisor_last_status(network: Chain, address: str) -> dict:
    try:
        # get hypervisor's last status found in database
        hypervisor_data = await local_database_helper(
            network=network
        ).get_items_from_database(
            collection_name="status",
            find={"address": address.lower()},
            sort=[("block", -1)],
            limit=1,
        )

        hypervisor_data = hypervisor_data[0]

    except Exception as err:
        pass

    return hypervisor_data


async def get_hypervisor_prices(hypervisor_address: str, network: Chain) -> dict:
    hype_last_status = await get_hypervisor_last_status(
        network=network, address=hypervisor_address
    )
    """ Get the latest hypervisor tokens and per share usd prices found in database. """

    price_token0, price_token1 = await asyncio.gather(
        global_database_helper().get_items_from_database(
            collection_name="usd_prices",
            find={
                "network": network.database_name,
                "address": hype_last_status["pool"]["token0"]["address"],
            },
            projection={"_id": 0, "id": 0, "address": 0, "network": 0},
            sort=[("block", -1)],
            limit=1,
        ),
        global_database_helper().get_items_from_database(
            collection_name="usd_prices",
            find={
                "network": network.database_name,
                "address": hype_last_status["pool"]["token1"]["address"],
            },
            projection={"_id": 0, "id": 0, "address": 0, "network": 0},
            sort=[("block", -1)],
            limit=1,
        ),
    )

    try:
        # convert hypervisor string to floats
        # hype_last_status["totalSupply"] = int(hype_last_status["totalSupply"]) / (
        #     10 ** hype_last_status["decimals"]
        # )

        price_lpToken = (
            price_token0[0]["price"]
            * (
                int(hype_last_status["totalAmounts"]["total0"])
                / (10 ** int(hype_last_status["pool"]["token0"]["decimals"]))
            )
            + price_token1[0]["price"]
            * (
                int(hype_last_status["totalAmounts"]["total1"])
                / (10 ** int(hype_last_status["pool"]["token1"]["decimals"]))
            )
        ) / (
            int(hype_last_status["totalSupply"]) / (10 ** hype_last_status["decimals"])
        )
    except Exception as err:
        price_lpToken = 0

    return {
        "address": hype_last_status["address"],
        "symbol": hype_last_status["symbol"],
        "share_price_usd": price_lpToken,
        "block": hype_last_status["block"],
        "token0": {
            "address": hype_last_status["pool"]["token0"]["address"],
            "symbol": hype_last_status["pool"]["token0"]["symbol"],
            "price_usd": price_token0[0],
        },
        "token1": {
            "address": hype_last_status["pool"]["token1"]["address"],
            "symbol": hype_last_status["pool"]["token1"]["symbol"],
            "price_usd": price_token1[0],
        },
    }


async def get_hypervisor_prices_historical(
    chain: Chain,
    hypervisor_address: str,
    block: int | None = None,
    timestamp: int | None = None,
) -> dict:
    """Get hypervisor share prices close to a timestamp or block from hypervisor returns collection.

    Args:
        chain (Chain): _description_
        hypervisor_address (str): _description_
        block (int | None, optional): _description_. Defaults to None.
        timestamp (int | None, optional): _description_. Defaults to None.

    Raises:
        ValueError: _description_

    Returns:
        dict: _description_
    """

    # either block or timestamp should be provided
    if not block and not timestamp:
        raise ValueError("Either block or timestamp should be provided")

    timevar_txt = "block" if block else "timestamp"
    timevar = block or timestamp

    _query = [
        {
            "$match": {
                "address": hypervisor_address.lower(),
                f"timeframe.end.{timevar_txt}": {"$lte": timevar},
            }
        },
        {"$sort": {"timeframe.ini.block": -1}},
        {"$limit": 1},
        {
            "$project": {
                "_id": 0,
                "address": "$address",
                # "as_close_as": {
                #     "$subtract": [timevar, f"$timeframe.end.{timevar_txt}"]
                # },
                "timeframe": "$timeframe",
                "share_price_usd_ini": {
                    "$divide": [
                        {
                            "$sum": [
                                {
                                    "$multiply": [
                                        "$status.ini.prices.token0",
                                        "$status.ini.underlying.qtty.token0",
                                    ]
                                },
                                {
                                    "$multiply": [
                                        "$status.ini.prices.token1",
                                        "$status.ini.underlying.qtty.token1",
                                    ]
                                },
                            ]
                        },
                        "$status.ini.supply",
                    ]
                },
                "share_price_usd_end": {
                    "$divide": [
                        {
                            "$sum": [
                                {
                                    "$multiply": [
                                        "$status.end.prices.token0",
                                        "$status.end.underlying.qtty.token0",
                                    ]
                                },
                                {
                                    "$multiply": [
                                        "$status.end.prices.token1",
                                        "$status.end.underlying.qtty.token1",
                                    ]
                                },
                            ]
                        },
                        "$status.end.supply",
                    ]
                },
                "token0_prce_usd_ini": "$status.ini.prices.token0",
                "token1_prce_usd_ini": "$status.ini.prices.token1",
                "token0_prce_usd_end": "$status.end.prices.token0",
                "token1_prce_usd_end": "$status.end.prices.token1",
            }
        },
    ]

    return [
        global_database_helper().convert_d128_to_decimal(x)
        for x in await local_database_helper(chain).get_items_from_database(
            collection_name="hypervisor_returns",
            aggregate=_query,
        )
    ]


async def get_hypervisor_rewards_status(
    network: Chain,
    hypervisor_address: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> list[dict]:
    find = {"hypervisor_address": hypervisor_address.lower()}

    if not start_timestamp and not end_timestamp and not start_block and not end_block:
        # DEFAULT RETURN LATEST REWARDS STATUS
        query = [
            {"$match": find},
            {"$sort": {"_id": -1}},
            {"$project": {"_id": 0, "id": 0, "rpc_costs": 0, "network": 0}},
        ]
        return await local_database_helper(network=network).get_items_from_database(
            collection_name="latest_reward_status",
            aggregate=query,
        )
    else:
        if start_block:
            find["block"] = {"$gte": start_block}
        elif start_timestamp:
            find["timestamp"] = {"$gte": start_timestamp}

        if end_block:
            find["block"] = {"$lte": end_block}
        if end_timestamp:
            find["timestamp"] = {"$lte": end_timestamp}

        return await local_database_helper(network=network).get_items_from_database(
            collection_name="rewards_status",
            find=find,
            sort=[("block", 1)],
            projection={"_id": 0, "id": 0},
        )


# TODO: delete or decide if we need it
async def add_prices_to_hypervisor(hypervisor: dict, network: str) -> dict:
    """Try to add usd prices for the hypervisor's tokens and LPtoken using database info only

    Args:
        hypervisor (dict): database hypervisor item
        network (str):

    Returns:
        dict: database hypervisor item with usd prices
    """

    try:
        # get token prices
        price_token0, price_token1 = await asyncio.gather(
            global_database_helper().get_price_usd(
                network=network.database_name,
                address=hypervisor["pool"]["token0"]["address"],
                block=hypervisor["block"],
            ),
            global_database_helper().get_price_usd(
                network=network.database_name,
                address=hypervisor["pool"]["token1"]["address"],
                block=hypervisor["block"],
            ),
        )
        price_token0 = price_token0[0]
        price_token1 = price_token1[0]

        # get LPtoken price
        price_lpToken = (
            price_token0
            * (
                int(hypervisor["totalAmounts"]["total0"])
                / (10 ** int(hypervisor["pool"]["token0"]["decimals"]))
            )
            + price_token1
            * (
                int(hypervisor["totalAmounts"]["total1"])
                / (10 ** int(hypervisor["pool"]["token1"]["decimals"]))
            )
        ) / (int(hypervisor["totalSupply"]) / (10 ** hypervisor["decimals"]))

        hypervisor["lpToken_price_usd"] = price_lpToken
        hypervisor["token0_price_usd"] = price_token0
        hypervisor["token1_price_usd"] = price_token1

    except Exception:
        pass

    return hypervisor
