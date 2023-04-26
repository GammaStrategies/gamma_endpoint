import asyncio
from fastapi import Response, APIRouter, status
from fastapi_cache.decorator import cache

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.subgraph.bins.common import (
    hypervisor,
    analytics,
    aggregate_stats,
    masterchef,
    masterchef_v2,
    users,
)
from sources.subgraph.bins.charts.daily import DailyChart
from sources.subgraph.bins.dashboard import Dashboard
from sources.subgraph.bins.eth import EthDistribution
from sources.subgraph.bins.gamma import GammaDistribution, GammaInfo, GammaYield
from sources.subgraph.bins.simulator import SimulatorInfo
from sources.subgraph.bins.config import (
    DEPLOYMENTS,
    RUN_FIRST_QUERY_TYPE,
    DEFAULT_TIMEZONE,
)
from sources.subgraph.bins.enums import Chain, Protocol, QueryType

from endpoint.config.cache import (
    ALLDATA_CACHE_TIMEOUT,
    APY_CACHE_TIMEOUT,
    DASHBOARD_CACHE_TIMEOUT,
    DB_CACHE_TIMEOUT,
    CHARTS_CACHE_TIMEOUT,
)

RUN_FIRST = RUN_FIRST_QUERY_TYPE


# Route builders


def build_routers() -> list:
    routes = []

    # all-deployments
    routes.append(
        subgraph_router_builder_allDeployments(
            tags=["All Deployments"], prefix="/allDeployments"
        )
    )

    # setup dex + chain endpoints
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.MAINNET,
            tags=["Uniswap - Ethereum"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.MAINNET.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.POLYGON,
            tags=["Uniswap - Polygon"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.ARBITRUM,
            tags=["Uniswap - Arbitrum"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.OPTIMISM,
            tags=["Uniswap - Optimism"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.OPTIMISM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.CELO,
            tags=["Uniswap - Celo"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.CELO.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.BSC,
            tags=["Uniswap - Binance"],
            prefix=f"/{Protocol.UNISWAP.value}/{Chain.BSC.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.QUICKSWAP,
            chain=Chain.POLYGON,
            tags=["Quickswap - Polygon"],
            prefix=f"/{Protocol.QUICKSWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.ZYBERSWAP,
            chain=Chain.ARBITRUM,
            tags=["Zyberswap - Arbitrum"],
            prefix=f"/{Protocol.ZYBERSWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.THENA,
            chain=Chain.BSC,
            tags=["Thena - BSC"],
            prefix=f"/{Protocol.THENA.value}/{Chain.BSC.value}",
        )
    )

    # Simulation
    routes.append(
        subgraph_router_builder_Simulator(tags=["Simulator"], prefix="/simulator")
    )

    # Charts
    routes.append(subgraph_router_builder_Charts(tags=["Charts"], prefix="/charts"))

    return routes


def build_routers_compatible() -> list:
    """Build backwards compatible routes for the old endpoint

    Returns:
        list: _description_
    """
    routes = []

    # all-deployments
    routes.append(
        subgraph_router_builder_allDeployments(
            tags=["All Deployments"], prefix="/allDeployments"
        )
    )

    # add Mainnet
    routes.append(
        subgraph_router_builder_compatible(
            dex=Protocol.UNISWAP,
            chain=Chain.MAINNET,
            tags=["Mainnet"],
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.POLYGON,
            tags=["Polygon"],
            prefix=f"/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.ARBITRUM,
            tags=["Arbitrum"],
            prefix=f"/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.OPTIMISM,
            tags=["Optimism"],
            prefix=f"/{Chain.OPTIMISM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.CELO,
            tags=["Celo"],
            prefix=f"/{Chain.CELO.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.BSC,
            tags=["BSC"],
            prefix=f"/{Chain.BSC.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.QUICKSWAP,
            chain=Chain.POLYGON,
            tags=["Quickswap - Polygon"],
            prefix=f"/{Protocol.QUICKSWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.ZYBERSWAP,
            chain=Chain.ARBITRUM,
            tags=["Zyberswap - Arbitrum"],
            prefix=f"/{Protocol.ZYBERSWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.THENA,
            chain=Chain.BSC,
            tags=["Thena - BSC"],
            prefix=f"/{Protocol.THENA.value}/{Chain.BSC.value}",
        )
    )

    # Simulation
    routes.append(
        subgraph_router_builder_Simulator(tags=["Simulator"], prefix="/simulator")
    )

    routes.append(subgraph_router_builder_Charts(tags=["Charts"], prefix="/charts"))

    return routes


# Route underlying functions


class subgraph_router_builder(router_builder_generalTemplate):
    # EXECUTION FUNCTIONS

    async def hypervisor_basic_stats(self, hypervisor_address: str, response: Response):
        return await hypervisor.hypervisor_basic_stats(
            self.dex, self.chain, hypervisor_address, response
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_returns(self, hypervisor_address: str, response: Response):
        hypervisor_returns = hypervisor.HypervisorsReturnsAllPeriods(
            protocol=self.dex,
            chain=self.chain,
            hypervisors=[hypervisor_address],
            response=response,
        )
        return await hypervisor_returns.run(RUN_FIRST)

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_average_returns(
        self, hypervisor_address: str, response: Response
    ):
        return await hypervisor.hypervisor_average_return(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            response=response,
        )

    async def hypervisor_uncollected_fees(
        self, hypervisor_address: str, response: Response
    ):
        return await hypervisor.uncollected_fees(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            response=response,
        )

    #    hypervisor analytics
    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_daily(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            chain=self.chain, hypervisor_address=hypervisor_address, period=1
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_weekly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            chain=self.chain, hypervisor_address=hypervisor_address, period=7
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_biweekly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            chain=self.chain, hypervisor_address=hypervisor_address, period=14
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_monthly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            chain=self.chain, hypervisor_address=hypervisor_address, period=30
        )

    #    hypervisors
    async def hypervisors_aggregate_stats(self, response: Response):
        result = aggregate_stats.AggregateStats(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await result.run(RUN_FIRST)

    async def hypervisors_recent_fees(self, response: Response, hours: int = 24):
        return await hypervisor.recent_fees(
            protocol=self.dex, chain=self.chain, hours=hours
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_returns(
        self, response: Response, apr_type: str | None = None
    ):
        """fee's Apr and Apy

        <apr_type> options are
          - **'users'**: calculate Apr and Apy using only Liquidity provider fees
          - **'gamma'**: calculate Apr and Apy using only Gamma fees
          - **'all'**  : calculate Apr and Apy using both Liquidity provider fees and Gamma fees

        """
        hypervisor_returns = hypervisor.HypervisorsReturnsAllPeriods(
            protocol=self.dex,
            chain=self.chain,
            hypervisors=None,
            apr_type=apr_type,
            response=response,
        )
        return await hypervisor_returns.run(RUN_FIRST)

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_average_returns(self, response: Response):
        return await hypervisor.hypervisors_average_return(
            protocol=self.dex, chain=self.chain, response=response
        )

    @cache(expire=ALLDATA_CACHE_TIMEOUT)
    async def hypervisors_all_data(self, response: Response):
        all_data = hypervisor.AllData(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await all_data.run(RUN_FIRST)

    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.uncollected_fees_all(
            protocol=self.dex,
            chain=self.chain,
        )

    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.collected_fees(
            protocol=self.dex,
            chain=self.chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_daily(self, response: Response):
        fee_returns = hypervisor.FeeReturns(
            protocol=self.dex, chain=self.chain, days=1, response=response
        )
        return await fee_returns.run(RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_weekly(self, response: Response):
        fee_returns = hypervisor.FeeReturns(
            protocol=self.dex, chain=self.chain, days=7, response=response
        )
        return await fee_returns.run(RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_monthly(self, response: Response):
        fee_returns = hypervisor.FeeReturns(
            protocol=self.dex, chain=self.chain, days=30, response=response
        )
        return await fee_returns.run(RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_daily(self, response: Response):
        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=1, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_weekly(self, response: Response):
        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=7, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_monthly(self, response: Response):
        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=30, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    # others
    async def hypervisors_rewards(self, response: Response):
        return await masterchef.info(protocol=self.dex, chain=self.chain)

    async def hypervisors_rewards2(self, response: Response):
        masterchef_v2_info = masterchef_v2.AllRewards2(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await masterchef_v2_info.run(RUN_FIRST)

    async def user_rewards(self, user_address: str, response: Response):
        return await masterchef.user_rewards(
            protocol=self.dex, chain=self.chain, user_address=user_address
        )

    async def user_rewards2(self, user_address: str, response: Response):
        return await masterchef_v2.user_rewards(
            protocol=self.dex, chain=self.chain, user_address=user_address
        )

    async def user_data(self, address: str, response: Response):
        return await users.user_data(
            protocol=self.dex, chain=self.chain, address=address
        )

    async def user_analytics(self, address: str, response: Response):
        return await users.get_user_analytic_data(
            chain=self.chain, address=address.lower()
        )

    async def vault_data(self, address: str, response: Response):
        return await users.account_data(
            protocol=self.dex, chain=self.chain, address=address
        )


class subgraph_router_builder_compatible(subgraph_router_builder):
    def _create_routes(self, dex, chain) -> APIRouter:
        """Create routes for the given chain and dex combination."""

        router = super()._create_routes(dex=dex, chain=chain)

        router.add_api_route(
            path="/visr/basicStats",
            endpoint=self.gamma_basic_stats,
            methods=["GET"],
        )
        router.add_api_route(
            path="/gamma/basicStats",
            endpoint=self.gamma_basic_stats,
            methods=["GET"],
        )
        router.add_api_route(
            path="/visr/yield",
            endpoint=self.gamma_yield,
            methods=["GET"],
        )
        router.add_api_route(
            path="/gamma/yield",
            endpoint=self.gamma_yield,
            methods=["GET"],
        )
        router.add_api_route(
            path="/{token_symbol}/dailyDistribution",
            endpoint=self.token_distributions,
            methods=["GET"],
        )
        router.add_api_route(
            path="/dashboard",
            endpoint=self.dashboard,
            methods=["GET"],
        )

        return router

    async def gamma_basic_stats(self, response: Response):
        result = GammaInfo(Chain.MAINNET, days=30)
        return await result.output()

    async def gamma_yield(self, response: Response):
        result = GammaYield(Chain.MAINNET, days=30)
        return await result.output()

    @cache(expire=DASHBOARD_CACHE_TIMEOUT)
    async def dashboard(self, response: Response, period: str = "weekly"):
        result = Dashboard(period.lower())

        return await result.info("UTC")

    async def token_distributions(
        self,
        response: Response,
        token_symbol: str = "gamma",
        days: int = 6,
        timezone: str = DEFAULT_TIMEZONE,
    ):
        token_symbol = token_symbol.lower()
        timezone = timezone.upper()

        if token_symbol not in ["gamma", "visr", "eth"]:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return "Only GAMMA, VISR and ETH supported"

        if timezone not in ["UTC", "UTC-5"]:
            response.status_code = status.HTTP_400_BAD_REQUEST
            return "Only UTC and UTC-5 timezones supported"

        distribution_class_map = {
            "gamma": GammaDistribution,
            "visr": GammaDistribution,
            "eth": EthDistribution,
        }

        token_distributions = distribution_class_map[token_symbol](
            self.chain, days=60, timezone=timezone
        )
        return await token_distributions.output(days)


class subgraph_router_builder_allDeployments(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        # ROOT
        router.add_api_route(
            path="/hypervisors/aggregateStats",
            endpoint=self.aggregate_stats,
            methods=["GET"],
        )

        router.add_api_route(
            path="/dashboard",
            endpoint=self.dashboard,
            methods=["GET"],
        )

        router.add_api_route(
            path="/gamma/basicStats",
            endpoint=self.gamma_basic_stats,
            methods=["GET"],
        )

        router.add_api_route(
            path="/gamma/yield",
            endpoint=self.gamma_yield,
            methods=["GET"],
        )

        return router

    async def aggregate_stats(
        self,
        response: Response,
    ) -> aggregate_stats.AggregateStatsDeploymentInfoOutput:
        results = await asyncio.gather(
            *[
                aggregate_stats.AggregateStats(
                    deployment[0], deployment[1], response
                ).run(RUN_FIRST)
                for deployment in DEPLOYMENTS
            ],
            return_exceptions=True,
        )

        valid_results = []
        included_deployments = []
        for index, result in enumerate(results):
            if not isinstance(result, Exception):
                valid_results.append(result)
                included_deployments.append(
                    f"{DEPLOYMENTS[index][0]}-{DEPLOYMENTS[index][1]}"
                )

        aggregated_results = sum(valid_results[1:], valid_results[0])

        return aggregate_stats.AggregateStatsDeploymentInfoOutput(
            totalValueLockedUSD=aggregated_results.totalValueLockedUSD,
            pairCount=aggregated_results.pairCount,
            totalFeesClaimedUSD=aggregated_results.totalFeesClaimedUSD,
            deployments=included_deployments,
        )

    async def gamma_basic_stats(self, response: Response):
        result = GammaInfo(Chain.MAINNET, days=30)
        return await result.output()

    async def gamma_yield(self, response: Response):
        result = GammaYield(Chain.MAINNET, days=30)
        return await result.output()

    @cache(expire=DASHBOARD_CACHE_TIMEOUT)
    async def dashboard(self, response: Response, period: str = "weekly"):
        result = Dashboard(period.lower())

        return await result.info("UTC")


class subgraph_router_builder_Simulator(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/tokenList",
            endpoint=self.token_list,
            methods=["GET"],
        )
        router.add_api_route(
            path="/poolTicks",
            endpoint=self.pool_ticks,
            methods=["GET"],
        )
        router.add_api_route(
            path="/poolFromTokens",
            endpoint=self.pool_from_tokens,
            methods=["GET"],
        )
        router.add_api_route(
            path="/pool24HrVolume",
            endpoint=self.pool_24hr_volume,
            methods=["GET"],
        )
        return router

    async def token_list(self):
        tokens = await SimulatorInfo(Protocol.UNISWAP, Chain.MAINNET).token_list()

        return tokens

    async def pool_ticks(self, poolAddress: str):
        ticks = await SimulatorInfo(Protocol.UNISWAP, Chain.MAINNET).pool_ticks(
            poolAddress
        )

        return ticks

    async def pool_from_tokens(self, token0: str, token1: str):
        pools = await SimulatorInfo(Protocol.UNISWAP, Chain.MAINNET).pools_from_tokens(
            token0, token1
        )

        return pools

    async def pool_24hr_volume(self, poolAddress: str):
        volume = await SimulatorInfo(Protocol.UNISWAP, Chain.MAINNET).pool_volume(
            poolAddress
        )

        return volume


class subgraph_router_builder_Charts(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/dailyTvl",
            endpoint=self.daily_tvl_chart_data,
            methods=["GET"],
        )
        router.add_api_route(
            path="/dailyFlows",
            endpoint=self.daily_flows_chart_data,
            methods=["GET"],
        )
        router.add_api_route(
            path="/dailyHypervisorFlows/{hypervisor_address}",
            endpoint=self.daily_hypervisor_flows_chart_data,
            methods=["GET"],
        )

        return router

    # Charts
    @cache(expire=CHARTS_CACHE_TIMEOUT)
    async def daily_tvl_chart_data(days: int = 24):
        daily = DailyChart(days)
        return {"data": await daily.tvl()}

    async def daily_flows_chart_data(days: int = 20):
        daily = DailyChart(days)
        return {"data": await daily.asset_flows()}

    async def daily_hypervisor_flows_chart_data(
        hypervisor_address: str, days: int = 20
    ):
        daily = DailyChart(days)
        return {"data": await daily.asset_flows(hypervisor_address)}
