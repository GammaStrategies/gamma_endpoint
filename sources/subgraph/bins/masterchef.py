from sources.subgraph.bins import GammaClient
from sources.subgraph.bins.constants import YEAR_SECONDS
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.pricing import token_prices


class MasterchefData:
    def __init__(self, protocol: Protocol, chain: Chain = Chain.MAINNET):
        self.chain = chain
        self.gamma_client = GammaClient(protocol, chain)
        self.data = {}

    async def _get_masterchef_data(self):
        query = """
        {
            masterChefs {
                id
                rewardPerBlock
                totalAllocPoint
                rewardToken{
                    id
                    symbol
                    decimals
                }
                pools {
                    id
                    allocPoint
                    lastRewardBlock
                    totalStaked
                    hypervisor{
                        id
                        symbol
                        pricePerShare
                    }
                }
            }
        }
        """

        response = await self.gamma_client.query(query)
        self.data = response["data"]["masterChefs"]

    async def _get_user_data(self, user_address):
        query = """
        query userRewards($userAddress: String!){
            account(id: $userAddress) {
                mcPoolAccounts {
                    amount
                    masterChefPool {
                        poolId
                        masterChef {
                            id
                            rewardToken {
                                id
                                symbol
                                decimals
                            }
                        }
                        hypervisor {
                            id
                            symbol
                        }
                    }
                }
            }
        }
        """
        variables = {"userAddress": user_address}

        response = await self.gamma_client.query(query, variables)
        self.data = response["data"]["account"]


class MasterchefInfo(MasterchefData):
    async def output(self, get_data=True):
        if get_data:
            await self._get_masterchef_data()

        info = {}
        prices = await token_prices(self.chain)
        for masterchef in self.data:
            reward_token_price = prices.get(masterchef["rewardToken"]["id"], 0)
            reward_per_second = (
                int(
                    masterchef["rewardPerBlock"]
                )  # rewardPerBlock is actually rewardPerSecond
                / 10 ** masterchef["rewardToken"]["decimals"]
            )

            pool_info = {}
            for pool in masterchef["pools"]:
                try:
                    apr = (
                        reward_token_price
                        * reward_per_second
                        * (int(pool["allocPoint"]) / int(masterchef["totalAllocPoint"]))
                        * YEAR_SECONDS
                        / (
                            int(pool["totalStaked"])
                            * float(pool["hypervisor"]["pricePerShare"])
                        )
                    )
                except ZeroDivisionError:
                    apr = 0
                pool_info[pool["hypervisor"]["id"]] = {
                    "hypervisorSymbol": pool["hypervisor"]["symbol"],
                    "allocPoint": pool["allocPoint"],
                    "lastRewardBlock": pool["lastRewardBlock"],
                    "apr": apr,
                }

            info[masterchef["id"]] = {
                "rewardToken": masterchef["rewardToken"]["id"],
                "rewardTokenSymbol": masterchef["rewardToken"]["symbol"],
                "rewardPerBlock": f"{reward_per_second:.30f}",
                "totalAllocPoint": masterchef["totalAllocPoint"],
                "pools": pool_info,
            }

        return info


class UserRewards(MasterchefData):
    def __init__(
        self, user_address: str, protocol: Protocol, chain: Chain = Chain.MAINNET
    ):
        super().__init__(protocol, chain)
        self.user_address = user_address.lower()

    async def output(self, get_data=True):
        if get_data:
            await self._get_user_data(self.user_address)

        if not self.data:
            return {}

        info = {}
        for pool in self.data["mcPoolAccounts"]:
            hypervisor_id = pool["masterChefPool"]["hypervisor"]["id"]
            hypervisor_symbol = pool["masterChefPool"]["hypervisor"]["symbol"]
            hypervisor_decimal = 18
            reward_token_id = pool["masterChefPool"]["masterChef"]["rewardToken"]["id"]
            reward_token_symbol = pool["masterChefPool"]["masterChef"]["rewardToken"][
                "symbol"
            ]

            if not info.get(hypervisor_id):
                info[hypervisor_id] = {"hypervisorSymbol": hypervisor_symbol}
            info[hypervisor_id][reward_token_id] = {
                "stakedAmount": int(pool["amount"]) / 10**hypervisor_decimal,
                "rewardTokenSymbol": reward_token_symbol,
            }

        return info
