import asyncio
from datetime import timedelta

import numpy as np
from pandas import DataFrame

from sources.subgraph.bins import GammaClient
from sources.subgraph.bins.config import EXCLUDED_HYPERVISORS, GROSS_FEES_MAX, TVL_MAX
from sources.subgraph.bins.constants import DAYS_IN_PERIOD
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.pricing import gamma_price
from sources.subgraph.bins.utils import filter_address_by_chain, timestamp_ago


class TopLevelData:
    """Top level stats"""

    def __init__(self, protocol: Protocol, chain: Chain = Chain.ETHEREUM):
        self.protocol = protocol
        self.chain = chain
        self.gamma_client = GammaClient(protocol, chain)
        self.all_stats_data = {}
        self.all_returns_data = {}

        self.excluded_hypervisors = filter_address_by_chain(EXCLUDED_HYPERVISORS, chain)

    async def get_hypervisor_data(self):
        """Get hypervisor IDs"""
        query = """
        {
            uniswapV3Hypervisors(
                first: 1000
            ){
                id
                grossFeesClaimedUSD
                tvlUSD
            }
        }
        """
        response = await self.gamma_client.query(query)
        return response["data"]["uniswapV3Hypervisors"]

    async def get_pool_data(self):
        query = """
        {
            uniswapV3Pools(
                first: 1000
            ){
                id
            }
        }
        """
        response = await self.gamma_client.query(query)
        return response["data"]["uniswapV3Pools"]

    async def _get_all_stats_data(self):
        query = """
        query allStats($grossFeesMax: Int!) {
            uniswapV3Hypervisors(
                first: 1000
            ){
                id
                grossFeesClaimedUSD
                tvlUSD
                badRebalances: rebalances(
                    where: {grossFeesUSD_gte: $grossFeesMax}
                ) {
                    grossFeesUSD
                    protocolFeesUSD
                    netFeesUSD
                }
            }
            uniswapV3Pools(
                first: 1000
            ){
                id
            }
        }
        """

        variables = {"grossFeesMax": GROSS_FEES_MAX}
        response = await self.gamma_client.query(query, variables)
        self.all_stats_data = response["data"]

    async def get_recent_rebalance_data(self, hours=24):
        query = """
        query rebalances($timestamp_start: Int!){
            uniswapV3Rebalances(
                first: 1000
                where: {
                    timestamp_gte: $timestamp_start
                }
            ) {
                grossFeesUSD
                protocolFeesUSD
                netFeesUSD
            }
        }
        """
        timestamp_start = timestamp_ago(timedelta(hours=hours))
        variables = {"timestamp_start": timestamp_start}
        response = await self.gamma_client.query(query, variables)
        return response["data"]["uniswapV3Rebalances"]

    def _all_stats(self):
        """
        Aggregate TVL and fees generated stats from all factories
        Should add entity to subgraph to track top level stats
        """
        data = self.all_stats_data

        total_tvl = 0
        for hypervisor in data["uniswapV3Hypervisors"]:
            if hypervisor["badRebalances"]:
                fees_correction_value = sum(
                    [
                        float(rebalance["grossFeesUSD"])
                        for rebalance in hypervisor["badRebalances"]
                    ]
                )
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - fees_correction_value,
                        0,
                    )
                )

            if (
                self.chain == Chain.BSC
                and self.protocol == Protocol.THENA
                and hypervisor["id"] == "0x01dd2d28eeb95d740acb5344b1e2c99b61cc3e64"
            ):
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"]) - 707293639.8442053,
                        0,
                    )
                )
            if (
                self.chain == Chain.BSC
                and self.protocol == Protocol.THENA
                and hypervisor["id"] == "0x99f5fd4588401e482d577a775b645c86678e308d"
            ):
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - 180446479065422783579566400230944600000
                        + 348.2528026369851712613650107144147,
                        0,
                    )
                )
            if (
                self.chain == Chain.BSC
                and self.protocol == Protocol.THENA
                and hypervisor["id"] == "0x5c11e97bc720e5c61afaa991f839dc6fdaa6cc00"
            ):
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - 204139348363971053920264706541381100000
                        + 292.9308911763711601986269689517349,
                        0,
                    )
                )
            if (
                self.chain == Chain.BSC
                and self.protocol == Protocol.THENA
                and hypervisor["id"] == "0xa614f6a18b484d9946024b2f48687853589f9296"
            ):
                hypervisor["grossFeesClaimedUSD"] = str(
                    max(
                        float(hypervisor["grossFeesClaimedUSD"])
                        - 132680098270120337796923875906769800
                        + 0.0005556920919671264040809536243588581,
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
