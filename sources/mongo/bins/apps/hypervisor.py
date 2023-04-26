from sources.common.general.enums import Chain, Dex, ChainId

from sources.common.database.collection_endpoint import database_global, database_local

# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


def create_local_database(network: Chain):
    """Create a local database for a hypervisor."""
    return database_local(mongo_url=MONGO_DB_URL, db_name=f"{network.value}_gamma")


def get_list(
    network: Chain,
    dex: Dex,
):
    return create_local_database(network=network).get_items_from_database(
        collection_name="static", find={"dex": dex.value}
    )


def get_uncollected_fees(
    network: Chain,
    hypervisor_address: str,
    timestamp: int | None = None,
    block: int | None = None,
) -> dict:
    """Get the uncollected fees for a hypervisor."""
    return create_local_database(network=network).get_items_from_database(
        collection_name="status",
        query=database_local.query_uncollected_fees(
            hypervisor_address=hypervisor_address, timestamp=timestamp, block=block
        ),
    )


def get_collected_fees(
    network: Chain,
    hypervisor_address: str,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
) -> dict:
    """Get the collected fees for a hypervisor."""
    return create_local_database(network=network).get_hypervisor_operations(
        collection_name="status",
        query=database_local.query_operations_summary(
            hypervisor_address=hypervisor_address,
            timestamp_ini=start_timestamp,
            timestamp_end=end_timestamp,
            block_ini=start_block,
            block_end=end_block,
        ),
    )
