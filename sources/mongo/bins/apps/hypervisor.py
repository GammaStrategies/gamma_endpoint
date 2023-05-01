import asyncio
from sources.common.general.enums import Chain, Dex, ChainId
from sources.common.database.collection_endpoint import database_global, database_local
from sources.mongo.bins.enums import enumsConverter

from sources.common.database.common.collections_common import db_collections_common

# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


def create_local_database(network: Chain):
    """Create a local database for a hypervisor."""
    chainvar = enumsConverter.convert_general_to_local(chain=network).value
    return database_local(
        mongo_url=MONGO_DB_URL,
        db_name=f"{enumsConverter.convert_general_to_local(chain=network).value}_gamma",
    )


def create_global_database():
    """Create a global database."""
    return database_global(mongo_url=MONGO_DB_URL)


async def hypervisors_list(network: Chain, dex: Dex):
    dexval = enumsConverter.convert_general_to_local(dex=dex).value
    return await create_local_database(network=network).get_items_from_database(
        collection_name="static",
        find={"dex": enumsConverter.convert_general_to_local(dex=dex).value},
        projection={"_id": 0},
    )


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
        for item in await create_local_database(
            network=network
        ).query_items_from_database(
            collection_name="status",
            query=database_local.query_uncollected_fees(
                hypervisor_address=hypervisor_address, timestamp=timestamp, block=block
            ),
        )
    ]


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
        for item in await create_local_database(
            network=network
        ).query_items_from_database(
            collection_name="status",
            query=database_local.query_uncollected_fees(
                timestamp=timestamp, block=block
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
        for item in await create_local_database(
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
        for item in await create_local_database(
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


async def get_hypervisor_last_status(network: Chain, dex: Dex, address: str) -> dict:
    dexval = enumsConverter.convert_general_to_local(dex=dex).value
    netval = enumsConverter.convert_general_to_local(chain=network).value

    local_db = create_local_database(network=netval)
    global_db = create_global_database()

    try:
        # get hypervisor's last status found in database
        hypervisor_data = await local_db.get_items_from_database(
            collection_name="status",
            find={"address": address.lower()},
            sort=[("block", -1)],
            limit=1,
        )[-1]

    except Exception:
        pass

    return hypervisor_data


async def add_prices_to_hypervisor(hypervisor: dict, network: str) -> dict:
    """Try to add usd prices for the hypervisor's tokens and LPtoken using database info only

    Args:
        hypervisor (dict): database hypervisor item
        network (str):

    Returns:
        dict: database hypervisor item with usd prices
    """
    global_db = create_global_database()

    try:
        # get token prices
        price_token0, price_token1 = await asyncio.gather(
            global_db.get_price_usd(
                network=network,
                address=hypervisor["pool"]["token0"]["address"],
                block=hypervisor["block"],
            ),
            global_db.get_price_usd(
                network=network,
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
        ) / (
            int(hypervisor["totalSupply"]["totalSupply"])
            / (10 ** hypervisor["decimals"])
        )

        hypervisor["lpToken_price_usd"] = price_lpToken
        hypervisor["token0_price_usd"] = price_token0
        hypervisor["token1_price_usd"] = price_token1

    except Exception:
        pass

    return hypervisor
