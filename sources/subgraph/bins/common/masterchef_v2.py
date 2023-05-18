import logging

from sources.subgraph.bins.database.managers import db_allRewards2_manager
from sources.subgraph.bins.common import ExecutionOrderWrapper
from sources.subgraph.bins.config import MONGO_DB_URL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.masterchef_v2 import MasterchefV2Info, UserRewardsV2

from sources.subgraph.bins.enums import enumsConverter as general_enumsConverter
from sources.mongo.bins.enums import enumsConverter as mongo_enumsConverter


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


async def user_rewards_thirdParty(user_address: str, protocol: Protocol, chain: Chain):
    result = []
    from sources.common.database.collection_endpoint import database_local

    db_name = f"{mongo_enumsConverter.convert_general_to_local(chain=general_enumsConverter.convert_local_to_general(chain=chain)).value}_gamma"
    db = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)

    # get all rewarders for this chain
    rewarders_addresses = []
    if rewarders_data := await db.get_items_from_database(
        collection_name="rewards_static",
        aggregate=db.query_rewarders_by_rewardRegistry(),
    ):
        for rewarders in rewarders_data:
            # append registry address
            rewarders_addresses.append(rewarders["_id"])
            # append rewarders addresses
            rewarders_addresses.extend(rewarders["rewarders"])

        user_rewards_data = await db.get_items_from_database(
            collection_name="operations",
            aggregate=db.query_user_allRewarder_transactions(
                user_address=user_address, rewarders_addresses=rewarders_addresses
            ),
        )

        # get all rewarders for this user
        for user_reward_status in [
            database_local.convert_d128_to_decimal(item=x) for x in user_rewards_data
        ]:
            result.append(
                {
                    "masterchef": user_reward_status["rewarder_data"][
                        "rewarder_registry"
                    ],
                    "poolId": user_reward_status["rewarder_data"]["rewarder_refIds"][0]
                    if user_reward_status["rewarder_data"]["rewarder_refIds"]
                    else -1,
                    "hypervisor": user_reward_status["hypervisor_data"]["address"],
                    "hypervisorSymbol": user_reward_status["hypervisor_data"]["symbol"],
                    "rewarder": user_reward_status["rewarder_data"]["rewarder_address"],
                    "rewardToken": user_reward_status["rewarder_data"]["rewardToken"],
                    "rewardTokenSymbol": user_reward_status["rewarder_data"][
                        "rewardToken_symbol"
                    ],
                    "stakedAmount": user_reward_status["staked"]
                    / (10 ** user_reward_status["hypervisor_data"]["decimals"]),
                }
            )

    return {"stakes": result}
