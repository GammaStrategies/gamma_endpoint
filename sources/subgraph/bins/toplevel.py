import asyncio
from datetime import timedelta

from gql.dsl import DSLQuery
import numpy as np
from pandas import DataFrame

from sources.subgraph.bins.config import EXCLUDED_HYPERVISORS, GROSS_FEES_MAX, TVL_MAX
from sources.subgraph.bins.constants import DAYS_IN_PERIOD
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.pricing import gamma_price
from sources.subgraph.bins.subgraphs.gamma import get_gamma_client
from sources.subgraph.bins.utils import filter_address_by_chain, timestamp_ago


class TopLevelData:
    """Top level stats"""

    def __init__(self, protocol: Protocol, chain: Chain = Chain.ETHEREUM):
        self.protocol = protocol
        self.chain = chain
        self.client = get_gamma_client(protocol, chain)
        self.all_stats_data = {}
        self.all_returns_data = {}

        self.excluded_hypervisors = filter_address_by_chain(EXCLUDED_HYPERVISORS, chain)

    async def get_hypervisor_data(self):
        ds = self.client.data_schema

        """Get hypervisor IDs"""
        query = DSLQuery(
            ds.Query.uniswapV3Hypervisors(first=1000).select(
                ds.UniswapV3Hypervisor.id,
                ds.UniswapV3Hypervisor.grossFeesClaimedUSD,
                ds.UniswapV3Hypervisor.tvlUSD
            )
        )
        
        response = await self.client.execute(query)
        return response["uniswapV3Hypervisors"]

    async def get_pool_data(self):
        ds = self.client.data_schema

        query = DSLQuery(
            ds.Query.uniswapV3Pools(first=1000).select(
                ds.UniswapV3Pool.id
            )
        )

        response = await self.client.execute(query)
        return response["uniswapV3Pools"]

    async def _get_all_stats_data(self):
        ds = self.client.data_schema

        query_rebal = DSLQuery(
            ds.Query.uniswapV3Hypervisors(first=1000).select(
                ds.UniswapV3Hypervisor.id,
                ds.UniswapV3Hypervisor.grossFeesClaimedUSD,
                ds.UniswapV3Hypervisor.tvlUSD,
                ds.UniswapV3Hypervisor.rebalances(
                    where={"grossFeesUSD_gte": GROSS_FEES_MAX}
                ).alias("badRebalances").select(
                    ds.UniswapV3Rebalance.grossFeesUSD,
                    ds.UniswapV3Rebalance.protocolFeesUSD,
                    ds.UniswapV3Rebalance.netFeesUSD,
                ),
            ),
            ds.Query.uniswapV3Pools(first=1000).select(
                ds.UniswapV3Pool.id
            )
        )

        query_zeroburn = DSLQuery(
            ds.Query.uniswapV3Hypervisors(first=1000).select(
                ds.UniswapV3Hypervisor.id,
                ds.UniswapV3Hypervisor.grossFeesClaimedUSD,
                ds.UniswapV3Hypervisor.tvlUSD,
                ds.UniswapV3Hypervisor.rebalances(
                    where={"grossFeesUSD_gte": GROSS_FEES_MAX}
                ).alias("badRebalances").select(
                    ds.UniswapV3Rebalance.grossFeesUSD,
                    ds.UniswapV3Rebalance.protocolFeesUSD,
                    ds.UniswapV3Rebalance.netFeesUSD,
                ),
                ds.UniswapV3Hypervisor.feeUpdates(
                    where={"grossFeesUSD_gte": GROSS_FEES_MAX}
                ).alias("badFees").select(
                    ds.UniswapV3FeeUpdate.feesUSD
                )
            ),
            ds.Query.uniswapV3Pools(first=1000).select(
                ds.UniswapV3Pool.id
            )
        )

        if self.protocol == Protocol.THENA and self.chain == Chain.BSC:
            query = query_zeroburn
        else:
            query = query_rebal

        # variables = {"grossFeesMax": GROSS_FEES_MAX}
        response = await self.client.execute(query)
        self.all_stats_data = response if response else {}

    async def get_recent_rebalance_data(self, hours=24):
        ds = self.client.data_schema

        query = DSLQuery(
            ds.Query.uniswapV3Rebalances(
                first=1000,
                where={
                    "timestamp_gte": timestamp_ago(timedelta(hours=hours))
                }
            ).select(
                ds.UniswapV3Rebalance.grossFeesUSD,
                ds.UniswapV3Rebalance.protocolFeesUSD,
                ds.UniswapV3Rebalance.netFeesUSD
            )
        )

        response = await self.client.execute(query)
        return response["uniswapV3Rebalances"]

    def _all_stats(self):
        """
        Aggregate TVL and fees generated stats from all factories
        Should add entity to subgraph to track top level stats
        """
        data = self.all_stats_data

        if not data:
            raise ValueError(f"Missing subgraph data for {self.protocol}/{self.chain}")

        total_tvl = 0

        for hypervisor in data["uniswapV3Hypervisors"]:
            if hypervisor["badRebalances"]:
                rebalance_fees_correction_value = sum(
                    [
                        float(rebalance["grossFeesUSD"])
                        for rebalance in hypervisor["badRebalances"]
                    ]
                )
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - rebalance_fees_correction_value,
                        0,
                    )
                )

            elif hypervisor.get("badFees"):
                fees_correction_value = sum(
                    [float(bad_fees["feesUSD"]) for bad_fees in hypervisor["badFees"]]
                )
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - fees_correction_value,
                        0,
                    )
                )

            if float(hypervisor["grossFeesClaimedUSD"]) > 10000000:
                hypervisor["grossFeesClaimedUSD"] = "0"

            hypervisor_tvl = float(hypervisor["tvlUSD"])
            if (hypervisor_tvl not in self.excluded_hypervisors) and (
                hypervisor_tvl < TVL_MAX
            ):
                total_tvl += hypervisor_tvl

        return {
            "pool_count": len(data["uniswapV3Pools"]),
            "hypervisor_count": len(data["uniswapV3Hypervisors"]),
            "tvl": total_tvl,
            "fees_claimed": sum(
                [
                    float(hypervisor["grossFeesClaimedUSD"])
                    for hypervisor in data["uniswapV3Hypervisors"]
                    if hypervisor["id"] not in self.excluded_hypervisors
                ]
            ),
        }

    async def all_stats(self):
        await self._get_all_stats_data()
        return self._all_stats()

    async def recent_fees(self, hours=24):
        data, gamma_prices = await asyncio.gather(
            self.get_recent_rebalance_data(hours), gamma_price()
        )
        gamma_price_usd = gamma_prices["token_in_usdc"]
        df_fees = DataFrame(data, dtype=np.float64)

        df_fees["grossFeesGAMMA"] = df_fees.grossFeesUSD / gamma_price_usd
        df_fees["protocolFeesGAMMA"] = df_fees.protocolFeesUSD / gamma_price_usd
        df_fees["netFeesGAMMA"] = df_fees.netFeesUSD / gamma_price_usd

        return df_fees.sum().to_dict()

    async def calculate_returns(
        self, period: str, current_timestamp: int | None = None
    ):
        hypervisors, all_returns = await asyncio.gather(
            self.get_hypervisor_data(),
            fee_returns_all(
                protocol=self.protocol,
                chain=self.chain,
                days=DAYS_IN_PERIOD[period],
                hypervisors=None,
                current_timestamp=current_timestamp,
            ),
        )

        tvl = sum(
            [
                float(hypervisor["tvlUSD"])
                for hypervisor in hypervisors
                if hypervisor["id"] not in self.excluded_hypervisors
            ]
        )

        returns = {"feeApr": 0, "feeApy": 0}
        for hypervisor in hypervisors:
            if hypervisor["id"] in self.excluded_hypervisors:
                continue
            if tvl > 0:
                tvl_share = float(hypervisor["tvlUSD"]) / tvl
            else:
                tvl_share = 0

            returns["feeApr"] += (
                all_returns["lp"].get(hypervisor["id"], {}).get("feeApr", 0) * tvl_share
            )
            returns["feeApy"] += (
                all_returns["lp"].get(hypervisor["id"], {}).get("feeApy", 0) * tvl_share
            )

        return returns
