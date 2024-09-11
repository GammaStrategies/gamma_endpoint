import asyncio
import logging

from sources.subgraph.bins.common.hypervisors import HYPE_TVL_MAX
from sources.subgraph.bins.common.hypervisors.schema import HypervisorBasicInfoOutput
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hypervisors.data import HypervisorAllData
from sources.subgraph.bins.hypervisors.schema import Hypervisor
from sources.subgraph.bins.pricing import token_prices
from sources.subgraph.bins.subgraphs.gamma import get_gamma_client
from sources.subgraph.bins.utils import timestamp_to_date

logger = logging.getLogger(__name__)


class BasicStats:
    """For processing basic stats of hypervisors"""
    def __init__(self, chain: Chain, protocol: Protocol):
        self.hype_data: dict[str, Hypervisor]
        self.hypervisors: list[str] | None = None
        self.prices: dict
        self.chain = chain
        self.protocol = protocol
        self.client = get_gamma_client(protocol, chain)

    async def get_data(self):
        hype_all_data = HypervisorAllData(self.chain, self.protocol)
        async with self.client.client as session:
            self.prices, _ = await asyncio.gather(
                token_prices(self.chain, self.protocol, session),
                hype_all_data.get_data(session=session, hypervisors=self.hypervisors)
            )

        self.hype_data = hype_all_data.data

    async def _process(self):
        await self.get_data()

        all_hypes = {}
        for hype_address, hype in self.hype_data.items():
            tick = 0

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

            all_hypes[hype_address] = HypervisorBasicInfoOutput(
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
                    f"{hype.total_supply / hype.max_total_supply}"
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
            )

        return all_hypes

    async def basic_stats(self, hypervisors: list[str] | None = None):
        if hypervisors:
            self.hypervisors = hypervisors

        return await self._process()
