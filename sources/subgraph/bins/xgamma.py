"""Data related to XGAMMA Staking"""

from dataclasses import InitVar, dataclass, field

from gql.dsl import DSLQuery

from sources.subgraph.bins.constants import XGAMMA_ADDRESS
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.schema import ValueWithDecimal
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.gamma import get_gamma_client


@dataclass
class XGammaInfo:
    """Data class to store query data"""

    gamma_staked: ValueWithDecimal = field(init=False)
    xgamma_supply: ValueWithDecimal = field(init=False)
    gamma_per_xgamma: float = field(init=False)
    gamma_staked_raw: InitVar[int]
    xgamma_supply_raw: InitVar[int]

    def __post_init__(self, gamma_staked_raw: int, xgamma_supply_raw: int):
        self.gamma_staked = ValueWithDecimal(gamma_staked_raw, decimals=18)
        self.xgamma_supply = ValueWithDecimal(xgamma_supply_raw, decimals=18)
        self.gamma_per_xgamma = self.gamma_staked.raw / self.xgamma_supply.raw


class XGammaData(SubgraphData):
    """Class to get xGamma staking relateed data"""

    def __init__(self):
        super().__init__()
        protocol = Protocol.UNISWAP
        chain = Chain.ETHEREUM
        self.data: XGammaInfo
        self.client = get_gamma_client(protocol, chain)

    async def query(self) -> dict:
        ds = self.client.data_schema

        return DSLQuery(
            ds.Query.rewardHypervisor(id=XGAMMA_ADDRESS).select(
                ds.RewardHypervisor.totalGamma,
                ds.RewardHypervisor.totalSupply,
            ),
        )

    def _transform_data(self) -> XGammaInfo:
        return XGammaInfo(
            gamma_staked_raw=self.query_response["rewardHypervisor"]["totalGamma"],
            xgamma_supply_raw=self.query_response["rewardHypervisor"]["totalSupply"],
        )
