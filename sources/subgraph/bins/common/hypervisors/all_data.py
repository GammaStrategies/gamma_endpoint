import asyncio
import logging

from gql.transport.exceptions import TransportQueryError
from pydantic import BaseModel

from sources.subgraph.bins.dex_pools.data import PoolData
from sources.subgraph.bins.dex_pools.schema import DexPool
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.hypervisors.data import HypervisorAllData
from sources.subgraph.bins.hypervisors.schema import Hypervisor
from sources.subgraph.bins.pricing import token_prices
from sources.subgraph.bins.utils import timestamp_to_date

# from fastapi import Response, status



logger = logging.getLogger(__name__)


class AllDataReturnsYield(BaseModel):
    feeApr: float
    feeApy: float


class AllDataReturns(BaseModel):
    daily: AllDataReturnsYield
    weekly: AllDataReturnsYield
    monthly: AllDataReturnsYield
    allTime: AllDataReturnsYield
    status: str


class AllDataOutput(BaseModel):
    createDate: str
    poolAddress: str
    name: str
    token0: str
    token1: str
    decimals0: int
    decimals1: int
    depositCap0: int
    depositCap1: int
    grossFeesClaimed0: float
    grossFeesClaimed1: float
    grossFeesClaimedUSD: str
    feesReinvested0: float
    feesReinvested1: float
    feesReinvestedUSD: str
    tvl0: float
    tvl1: float
    tvlUSD: str
    totalSupply: int
    maxTotalSupply: int
    capacityUsed: str
    sqrtPrice: str
    tick: int
    baseLower: int
    baseUpper: int
    inRange: bool
    observationIndex: str
    poolTvlUSD: str
    poolFeesUSD: str
    returns: AllDataReturns


class AllData:
    def __init__(self, chain: Chain, protocol: Protocol):
        self.hype_data: dict[str, Hypervisor]
        self.pool_data: dict[str, DexPool]
        self.fee_yield: dict[str, dict]
        self.prices: dict
        self.chain = chain
        self.protocol = protocol

    async def _get_subgraph_data(self):
        hype_data = HypervisorAllData(self.chain, self.protocol)
        pool_data = PoolData(self.chain, self.protocol)

        await hype_data.get_data()
        self.hype_data = hype_data.data

        pools = [hype.pool for hype in hype_data.data.values()]
        try:
            await pool_data.get_data(pools=pools)
        except TransportQueryError:
            self.pool_data = {}
            logger.warning(
                "Failed to get Pool Data for %s - %s", self.protocol.value, self.chain.value
            )
        else:
            self.pool_data = pool_data.data

    async def get_data(self):
        self.prices, fee_yield, _ = await asyncio.gather(
            token_prices(self.chain, self.protocol),
            fee_returns_all(
                protocol=self.protocol,
                chain=self.chain,
                days=1,
                hypervisors=None,
                current_timestamp=None,
            ),
            self._get_subgraph_data(),
        )

        self.fee_yield = fee_yield["lp"]

    async def output(self):
        await self.get_data()

        all_data = {}
        for hype_address, hype in self.hype_data.items():
            pool = self.pool_data.get(hype.pool)
            tick = pool.tick if pool else 0

            hype_returns = self.fee_yield.get(hype_address, {})

            returns_yield = AllDataReturnsYield(
                feeApr=hype_returns.get("feeApr", 0),
                feeApy=hype_returns.get("feeApy", 0),
            )

            returns = AllDataReturns(
                daily=returns_yield,
                weekly=returns_yield,
                monthly=returns_yield,
                allTime=returns_yield,
                status=hype_returns.get(
                    "hasOutlier", hype_returns.get("status", "no data")
                ),
            )

            all_data[hype_address] = AllDataOutput(
                createDate=timestamp_to_date(hype.created, "%d %b, %Y"),
                poolAddress=hype.pool,
                name=f"{hype.token_0.symbol}-{hype.token_1.symbol}-{hype.pool_fee}",
                token0=hype.token_0.address,
                token1=hype.token_1.address,
                decimals0=hype.token_0.decimals,
                decimals1=hype.token_1.decimals,
                depositCap0=hype.deposit_max.value0.adjusted,
                depositCap1=hype.deposit_max.value1.adjusted,
                grossFeesClaimed0=hype.gross_fees_claimed.value0.adjusted,
                grossFeesClaimed1=hype.gross_fees_claimed.value1.adjusted,
                grossFeesClaimedUSD=hype.gross_fees_claimed_usd,
                feesReinvested0=hype.fees_reinvested.value0.adjusted,
                feesReinvested1=hype.fees_reinvested.value1.adjusted,
                feesReinvestedUSD=hype.fees_reinvested_usd,
                tvl0=hype.tvl.value0.adjusted,
                tvl1=hype.tvl.value1.adjusted,
                tvlUSD=hype.tvl_usd,
                totalSupply=hype.total_supply,
                maxTotalSupply=hype.max_total_supply,
                capacityUsed=hype.total_supply / hype.max_total_supply
                if hype.max_total_supply > 0
                else "No cap",
                sqrtPrice=hype.pool_price,
                tick=tick,
                baseLower=hype.base_lower,
                baseUpper=hype.base_upper,
                inRange=bool(hype.base_lower <= tick <= hype.base_upper),
                observationIndex=pool.observation_index if pool else 0,
                poolTvlUSD=pool.total_value_locked_usd if pool else 0,
                poolFeesUSD=pool.fees_usd if pool else 0,
                returns=returns,
            )

        return all_data
