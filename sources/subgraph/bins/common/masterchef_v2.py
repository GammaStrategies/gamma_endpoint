import logging

from sources.subgraph.bins.database.managers import db_allRewards2_manager
from sources.subgraph.bins.common import ExecutionOrderWrapper
from sources.subgraph.bins.config import MONGO_DB_URL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.masterchef_v2 import MasterchefV2Info, UserRewardsV2

logger = logging.getLogger(__name__)


class AllRewards2(ExecutionOrderWrapper):
    async def _database(self):
        _mngr = db_allRewards2_manager(mongo_url=MONGO_DB_URL)
        result = await _mngr.get_last_data(chain=self.chain, protocol=self.protocol)
        if result == {}:
            raise ValueError(" no data in database ?")
        self.database_datetime = result.pop("datetime", "")
        return result

    async def _subgraph(self):
        masterchef_info = MasterchefV2Info(self.protocol, self.chain)
        return await masterchef_info.output(get_data=True)


async def user_rewards(protocol: Protocol, chain: Chain, user_address: str):
    user_rewards = UserRewardsV2(user_address, protocol, chain)
    return await user_rewards.output(get_data=True)
