from fastapi import Response

from sources.common.database.collection_endpoint import database_local
from sources.subgraph.bins.accounts import AccountInfo
from sources.subgraph.bins.config import MONGO_DB_URL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.users import UserInfo


async def user_data(protocol: Protocol, chain: Chain, address: str):
    user_info = UserInfo(protocol, chain, address)
    return await user_info.output(get_data=True)


async def account_data(protocol: Protocol, chain: Chain, address: str):
    account_info = AccountInfo(protocol, chain, address)
    return await account_info.output()


from sources.mongo.bins.apps.user import (
    get_user_analytic_data as get_user_analytic_data_mongo,
)


async def get_user_analytic_data(
    chain: Chain,
    address: str,
    block_ini: int = 0,
    block_end: int = 0,
    response: Response | None = None,
):
    # db_name = f"{chain.database_name}_gamma"
    # db = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)

    # return await db.get_user_status(
    #     address=address, block_ini=block_ini, block_end=block_end
    # )
    return await get_user_analytic_data_mongo(
        chain=chain, address=address, block_ini=block_ini, block_end=block_end
    )
