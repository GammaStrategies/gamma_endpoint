from gql.dsl import DSLQuery

from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.dex_pools.schema import DexPool
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.uniswap_v3 import UniswapClient


class PoolData(SubgraphData):
    """Class to get xGamma staking relateed data"""

    def __init__(self, chain: Chain, protocol: Protocol):
        super().__init__()
        self.data: {}
        self.client = UniswapClient(protocol, chain)

    async def _query_data(self, pools: list[str] | None = None) -> dict:
        ds = self.client.data_schema
        pool_filter = {"where": {"id_in": pools}} if pools else {}

        query = DSLQuery(
            ds.Query.pools(**({"first": 1000} | pool_filter)).select(
                ds.Pool.id,
                ds.Pool.sqrtPrice,
                ds.Pool.tick,
                ds.Pool.observationIndex,
                ds.Pool.feesUSD,
                ds.Pool.totalValueLockedUSD,
            )
        )

        response = await self.client.execute(query)
        self.query_response = response

    def _transform_data(self) -> dict[str, DexPool]:
        pools = {
            pool["id"]: DexPool(
                address=pool["id"],
                sqrt_price=pool["sqrtPrice"],
                tick=pool["tick"] if pool["tick"] else 0,
                observation_index=pool["observationIndex"],
                fees_usd=pool["feesUSD"],
                total_value_locked_usd=pool["totalValueLockedUSD"],
            )
            for pool in self.query_response["pools"]
        }

        return pools
