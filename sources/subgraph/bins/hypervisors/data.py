from gql.dsl import DSLQuery

from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hypervisors.schema import Hypervisor
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.gamma import GammaClient


class HypervisorAllData(SubgraphData):
    """Class to get hypervisor related data"""

    def __init__(self, chain: Chain, protocol: Protocol):
        super().__init__()
        self.chain = chain
        self.protocol = protocol
        self.data: {}
        self.client = GammaClient(protocol, chain)

    async def _query_data(self, hypervisors: list[str] | None = None) -> dict:
        ds = self.client.data_schema
        hypervisor_filter = {"where": {"id_in": hypervisors}} if hypervisors else {}

        query = DSLQuery(
            ds.Query.uniswapV3Hypervisors(**({"first": 1000} | hypervisor_filter)).select(
                ds.UniswapV3Hypervisor.id,
                ds.UniswapV3Hypervisor.created,
                ds.UniswapV3Hypervisor.baseLower,
                ds.UniswapV3Hypervisor.baseUpper,
                ds.UniswapV3Hypervisor.totalSupply,
                ds.UniswapV3Hypervisor.maxTotalSupply,
                ds.UniswapV3Hypervisor.deposit0Max,
                ds.UniswapV3Hypervisor.deposit1Max,
                ds.UniswapV3Hypervisor.grossFeesClaimed0,
                ds.UniswapV3Hypervisor.grossFeesClaimed1,
                ds.UniswapV3Hypervisor.grossFeesClaimedUSD,
                ds.UniswapV3Hypervisor.feesReinvested0,
                ds.UniswapV3Hypervisor.feesReinvested1,
                ds.UniswapV3Hypervisor.feesReinvestedUSD,
                ds.UniswapV3Hypervisor.tvl0,
                ds.UniswapV3Hypervisor.tvl1,
                ds.UniswapV3Hypervisor.tvlUSD,

                ds.UniswapV3Hypervisor.pool.select(
                    ds.UniswapV3Pool.id,
                    ds.UniswapV3Pool.fee,
                    ds.UniswapV3Pool.sqrtPriceX96,
                    ds.UniswapV3Pool.token0.select(
                        ds.Token.id,
                        ds.Token.symbol,
                        ds.Token.decimals
                    ),
                    ds.UniswapV3Pool.token1.select(
                        ds.Token.id,
                        ds.Token.symbol,
                        ds.Token.decimals
                    ),
                )
            )
        )

        response = await self.client.execute(query)
        self.query_response = response

    def _transform_data(self) -> dict[str, Hypervisor]:
        hypervisors = {
            hype["id"]: Hypervisor(
                address=hype["id"],
                created=hype["created"],
                base_lower=hype["baseLower"],
                base_upper=hype["baseUpper"],
                total_supply=hype["totalSupply"],
                max_total_supply=hype["maxTotalSupply"],
                deposit_max_0=hype["deposit0Max"],
                deposit_max_1=hype["deposit1Max"],
                gross_fees_claimed_0=hype["grossFeesClaimed0"],
                gross_fees_claimed_1=hype["grossFeesClaimed1"],
                gross_fees_claimed_usd=hype["grossFeesClaimedUSD"],
                fees_reinvested_0=hype["feesReinvested0"],
                fees_reinvested_1=hype["feesReinvested1"],
                fees_reinvested_usd=hype["feesReinvestedUSD"],
                tvl_0=hype["tvl0"],
                tvl_1=hype["tvl1"],
                tvl_usd=hype["tvlUSD"],
                pool=hype["pool"]["id"],
                pool_fee=hype["pool"]["fee"],
                pool_price=hype["pool"]["sqrtPriceX96"],
                token_0_address=hype["pool"]["token0"]["id"],
                token_0_symbol=hype["pool"]["token0"]["symbol"],
                token_0_decimals=hype["pool"]["token0"]["decimals"],
                token_1_address=hype["pool"]["token1"]["id"],
                token_1_symbol=hype["pool"]["token1"]["symbol"],
                token_1_decimals=hype["pool"]["token1"]["decimals"]
            ) for hype in self.query_response["uniswapV3Hypervisors"]
        }

        xpsdn_eth1_hype = "0x0ec4a47065bf52e1874d2491d4deeed3c638c75f"
        if self.chain == Chain.ETHEREUM:
            if hypervisors.get(xpsdn_eth1_hype):
                hypervisors[xpsdn_eth1_hype].gross_fees_claimed_usd -= 238300
                hypervisors[xpsdn_eth1_hype].fees_reinvested_usd -= 214470

        return hypervisors
    