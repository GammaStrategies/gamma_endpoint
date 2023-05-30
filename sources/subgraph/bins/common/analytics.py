from datetime import datetime, timedelta
from fastapi import Response, status

from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.database.managers import (
    db_returns_manager,
    db_allRewards2_manager,
)
from sources.subgraph.bins.config import MASTERCHEF_ADDRESSES, MONGO_DB_URL


class HypervisorAnalytics:
    def __init__(self, chain: Chain, protocol: Protocol, hypervisor_address: str):
        self.chain = chain
        self.protocol = protocol
        self.address = hypervisor_address.lower()

        self.returns_manager = db_returns_manager(mongo_url=MONGO_DB_URL)
        self.allrewards2_manager = db_allRewards2_manager(mongo_url=MONGO_DB_URL)

    async def get_data(
        self,
        period: int = 30,
    ):
        end_date = datetime.now()
        ini_date = end_date - timedelta(days=period)

        return await self.returns_manager._get_data(
            query=self.returns_manager.query_return_imperm_rewards2_flat(
                chain=self.chain,
                hypervisor_address=self.address,
                period=period,
                ini_date=ini_date,
                end_date=end_date,
                filter_valid_masterchef=self.protocol
                in MASTERCHEF_ADDRESSES.get(self.chain, {}),
            )
        )


async def get_hype_data(
    protocol: Protocol,
    chain: Chain,
    hypervisor_address: str,
    period: int,
    response: Response = None,
):
    if response:
        # this is a database query only
        response.headers["X-Database"] = "true"
    atest = HypervisorAnalytics(
        chain=chain, protocol=protocol, hypervisor_address=hypervisor_address
    )
    return await atest.get_data(period=period)
