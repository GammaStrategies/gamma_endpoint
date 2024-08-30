import asyncio
import logging

from pydantic import BaseModel

from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.hypervisors.data import HypervisorAllData
from sources.subgraph.bins.hypervisors.schema import Hypervisor
from sources.subgraph.bins.pricing import token_prices
from sources.subgraph.bins.subgraphs.gamma import get_gamma_client
from sources.subgraph.bins.utils import timestamp_to_date

# from fastapi import Response, status

HYPE_TVL_MAX = 1000000000

logger = logging.getLogger(__name__)


class HypervisorBasicInfoOutput(BaseModel):
    """Output schema for Hypervisor Basic Stats"""

    createDate: str
    poolAddress: str
    name: str
    token0: str
    token1: str
    decimals0: int
    decimals1: int
    depositCap0: float
    depositCap1: float
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

class AllDataReturnsYield(BaseModel):
    feeApr: float
    feeApy: float


class AllDataReturns(BaseModel):
    daily: AllDataReturnsYield
    weekly: AllDataReturnsYield
    monthly: AllDataReturnsYield
    allTime: AllDataReturnsYield
    status: str


class AllDataOutput(HypervisorBasicInfoOutput):
    returns: AllDataReturns


class AllData:
    def __init__(self, chain: Chain, protocol: Protocol):
        self.hype_data: dict[str, Hypervisor]
        self.fee_yield: dict[str, dict]
        self.hypervisors: list[str] | None = None
        self.prices: dict
        self.chain = chain
        self.protocol = protocol
        self.client = get_gamma_client(protocol, chain)

    async def _get_subgraph_data(self, session=None):
        hype_data = HypervisorAllData(self.chain, self.protocol)

        await hype_data.get_data(session=session, hypervisors=self.hypervisors)
        self.hype_data = hype_data.data

    async def get_data(self):
        async with self.client.client as session:
            self.prices, fee_yield, _ = await asyncio.gather(
                token_prices(self.chain, self.protocol, session),
                fee_returns_all(
                    protocol=self.protocol,
                    chain=self.chain,
                    days=1,
                    hypervisors=self.hypervisors,
                    current_timestamp=None,
                    session=session,
                ),
                self._get_subgraph_data(session),
            )

        self.fee_yield = fee_yield["lp"]

    async def _process(self):
        await self.get_data()

        all_hypes = {}
        for hype_address, hype in self.hype_data.items():
            tick = 0

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

            # Override TVL USD if necessary with api pricing
            tvl0 = hype.tvl.value0.adjusted
            tvl1 = hype.tvl.value1.adjusted

            if (hype.tvl_usd == 0 and (tvl0 > 0 or tvl1 > 0)) or (
                hype.tvl_usd > HYPE_TVL_MAX
            ):
                tvl_usd = tvl0 * self.prices.get(
                    hype.token_0.address, 0
                ) + tvl1 * self.prices.get(hype.token_1.address, 0)
            else:
                tvl_usd = hype.tvl_usd

            all_hypes[hype_address] = AllDataOutput(
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
                grossFeesClaimedUSD=str(hype.gross_fees_claimed_usd),
                feesReinvested0=hype.fees_reinvested.value0.adjusted,
                feesReinvested1=hype.fees_reinvested.value1.adjusted,
                feesReinvestedUSD=str(hype.fees_reinvested_usd),
                tvl0=hype.tvl.value0.adjusted,
                tvl1=hype.tvl.value1.adjusted,
                tvlUSD=str(tvl_usd),
                totalSupply=hype.total_supply,
                maxTotalSupply=hype.max_total_supply,
                capacityUsed=(
                    hype.total_supply / hype.max_total_supply
                    if hype.max_total_supply > 0
                    else "No cap"
                ),
                sqrtPrice=str(hype.pool_price),
                tick=tick,
                baseLower=hype.base_lower,
                baseUpper=hype.base_upper,
                inRange=bool(hype.base_lower <= tick <= hype.base_upper),
                observationIndex="0",
                poolTvlUSD="0",
                poolFeesUSD="0",
                returns=returns,
            )

        return all_hypes

    async def all_data(self):
        return await self._process()

    async def basic_stats(self, hypervisor: str):
        self.hypervisors = [hypervisor]
        hypervisors_data = await self._process()

        hype = hypervisors_data[hypervisor]

        return HypervisorBasicInfoOutput(
            createDate=hype.createDate,
            poolAddress=hype.poolAddress,
            name=hype.name,
            token0=hype.token0,
            token1=hype.token1,
            decimals0=hype.decimals0,
            decimals1=hype.decimals1,
            depositCap0=hype.depositCap0,
            depositCap1=hype.depositCap1,
            grossFeesClaimed0=hype.grossFeesClaimed0,
            grossFeesClaimed1=hype.grossFeesClaimed1,
            grossFeesClaimedUSD=hype.grossFeesClaimedUSD,
            feesReinvested0=hype.feesReinvested0,
            feesReinvested1=hype.feesReinvested1,
            feesReinvestedUSD=hype.feesReinvestedUSD,
            tvl0=hype.tvl0,
            tvl1=hype.tvl1,
            tvlUSD=hype.tvlUSD,
            totalSupply=hype.totalSupply,
            maxTotalSupply=hype.maxTotalSupply,
            capacityUsed=hype.capacityUsed,
            sqrtPrice=hype.sqrtPrice,
            tick=hype.tick,
            baseLower=hype.baseLower,
            baseUpper=hype.baseUpper,
            inRange=hype.inRange,
            observationIndex=hype.observationIndex,
            poolTvlUSD=hype.poolTvlUSD,
            poolFeesUSD=hype.poolFeesUSD,
        )
