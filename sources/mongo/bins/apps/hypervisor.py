import asyncio
from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import database_global, database_local

from sources.common.database.common.collections_common import db_collections_common

from sources.web3.bins.database.db_raw_direct_info import direct_db_hypervisor_info


# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL

# General database helpers


def local_database_helper(network: Chain):
    """Create a local database for a hypervisor."""
    return database_local(
        mongo_url=MONGO_DB_URL,
        db_name=f"{network.database_name}_gamma",
    )


def global_database_helper():
    """Create a global database."""
    return database_global(mongo_url=MONGO_DB_URL)


# Hypervisors


async def hypervisors_list(network: Chain, protocol: Protocol):
    return await local_database_helper(network=network).get_items_from_database(
        collection_name="static",
        find={"dex": protocol.database_name},
        projection={"_id": 0},
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
                hypervisor_address=hypervisor_address,
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


async def get_hypervisor_return(
    network: Chain,
    hypervisor_address: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict:
    hype_helper = direct_db_hypervisor_info(
        hypervisor_address=hypervisor_address.lower(),
        network=network.database_name,
        protocol="gamma",
    )

    # result = hype_helper.get_feeReturn_and_IL(
    #     ini_date=hype_ini_date, end_date=hype_end_date
    # )


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
        # DEFAULT RETURN LAST REWARDS STATUS GROUPED BY BLOCK
        query = [
            {"$match": find},
            {
                "$group": {
                    "_id": "$block",
                    "data": {"$push": "$$ROOT"},
                },
            },
            {"$sort": {"_id": -1}},
            {"$limit": 1},
            {"$unwind": "$data"},
            {"$replaceRoot": {"newRoot": "$data"}},
            {"$project": {"_id": 0, "id": 0}},
        ]
        return await local_database_helper(network=network).get_items_from_database(
            collection_name="rewards_status",
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
