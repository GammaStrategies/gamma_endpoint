from sources.subgraph.bins import GammaClient
from sources.subgraph.bins.accounts import AccountInfo
from sources.subgraph.bins.enums import Chain, Protocol


class UserData:
    def __init__(self, protocol: Protocol, chain: Chain, user_address: str):
        self.protocol = protocol
        self.chain = chain
        self.gamma_client = GammaClient(protocol, chain)
        self.gamma_client_mainnet = GammaClient(Protocol.UNISWAP, Chain.ETHEREUM)
        self.address = user_address.lower()
        self.decimal_factor = 10**18
        self.data = {}

    async def _get_data(self):
        query = """
        query userHypervisor($userAddress: String!) {
            user(
                id: $userAddress
            ){
                accountsOwned {
                    id
                    parent { id }
                    hypervisorShares {
                        hypervisor {
                            id
                            pool{
                                token0{ decimals }
                                token1{ decimals }
                            }
                            conversion {
                                baseTokenIndex
                                priceTokenInBase
                                priceBaseInUSD
                            }
                            totalSupply
                            tvl0
                            tvl1
                            tvlUSD
                        }
                        shares
                        initialToken0
                        initialToken1
                        initialUSD
                    }
                }
            }
        }
        """
        variables = {"userAddress": self.address}

        hypervisor_response = await self.gamma_client.query(query, variables)

        self.data = {
            "hypervisor": hypervisor_response["data"],
        }


class UserInfo(UserData):
    async def output(self, get_data=True):
        if get_data:
            await self._get_data()

        hypervisor_data = self.data["hypervisor"]

        has_hypervisor_data = hypervisor_data.get("user")

        if not has_hypervisor_data:
            return {}

        if has_hypervisor_data:
            hypervisor_lookup = {
                account.pop("id"): account
                for account in hypervisor_data["user"]["accountsOwned"]
            }
        else:
            hypervisor_lookup = {}

        # combine accounts owned for both hype and xgamma
        all_accounts = set(list(hypervisor_lookup.keys()))

        accounts = {}
        # for accountHypervisor in hypervisor_data["user"]["accountsOwned"]:
        for account_address in all_accounts:
            # account_address = accountHypervisor["id"]
            account_info = AccountInfo(self.protocol, self.chain, account_address)
            account_info.data = {
                "hypervisor": {"account": hypervisor_lookup.get(account_address)},
            }
            accounts[account_address] = await account_info.output(get_data=False)

        return accounts
