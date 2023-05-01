from sources.common.general.enums import Chain, Dex, ChainId
from sources.common.database.collection_endpoint import database_global, database_local
from sources.mongo.bins.enums import enumsConverter

# TODO: restruct global config and local config
from sources.subgraph.bins.config import MONGO_DB_URL


async def get_user_analytic_data(
    chain: Chain,
    address: str,
    block_ini: int = 0,
    block_end: int = 0,
):
    db_name = f"{enumsConverter.convert_local_to_general(chain=chain).value}_gamma"
    local_db_helper = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)
    return await local_db_helper.get_user_status(
        address=address, block_ini=block_ini, block_end=block_end
    )


async def get_user_historic_info(
    chain: Chain, address: str, timestamp_ini: int = 0, timestamp_end: int = 0
):
    db_name = f"{enumsConverter.convert_local_to_general(chain=chain).value}_gamma"
    local_db_helper = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)

    return await local_db_helper.get_user_operations_status(
        user_address=address, timestamp_ini=timestamp_ini, timestamp_end=timestamp_end
    )
