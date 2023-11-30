import asyncio

from fastapi import APIRouter, Response, status
from fastapi_cache.decorator import cache

from endpoint.config.cache import (
    ALLDATA_CACHE_TIMEOUT,
    APY_CACHE_TIMEOUT,
    CHARTS_CACHE_TIMEOUT,
    DASHBOARD_CACHE_TIMEOUT,
    DB_CACHE_TIMEOUT,
)
from endpoint.routers.template import (
    router_builder_baseTemplate,
    router_builder_generalTemplate,
)
from sources.common.general.enums import Period
from sources.subgraph.bins.charts.daily import DailyChart
from sources.subgraph.bins.common import (
    SubgraphStatusOutput,
    aggregate_stats,
    analytics,
    charts,
    hypervisor,
    masterchef,
    masterchef_v2,
    subgraph_status,
    users,
)
from sources.subgraph.bins.common.hypervisor import (
    hypervisor_basic_stats as basic_stats_output,
)
from sources.subgraph.bins.config import (
    DEFAULT_TIMEZONE,
    DEPLOYMENTS,
    RUN_FIRST_QUERY_TYPE,
    THIRD_PARTY_REWARDERS,
)
from sources.subgraph.bins.dashboard import Dashboard
from sources.subgraph.bins.enums import Chain, Protocol, QueryType
from sources.subgraph.bins.eth import EthDistribution
from sources.subgraph.bins.gamma import GammaDistribution, GammaInfo, GammaYield
from sources.subgraph.bins.simulator import SimulatorInfo

RUN_FIRST = RUN_FIRST_QUERY_TYPE


# Route builders

DEPLOYED: list[tuple[Protocol, Chain]] = [
    (Protocol.UNISWAP, Chain.ETHEREUM),
    (Protocol.UNISWAP, Chain.POLYGON),
    (Protocol.UNISWAP, Chain.ARBITRUM),
    (Protocol.UNISWAP, Chain.OPTIMISM),
    (Protocol.UNISWAP, Chain.CELO),
    (Protocol.UNISWAP, Chain.BSC),
    (Protocol.UNISWAP, Chain.MOONBEAM),
    (Protocol.QUICKSWAP, Chain.POLYGON),
    (Protocol.QUICKSWAP, Chain.POLYGON_ZKEVM),
    (Protocol.ZYBERSWAP, Chain.ARBITRUM),
    (Protocol.THENA, Chain.BSC),
    (Protocol.CAMELOT, Chain.ARBITRUM),
    (Protocol.GLACIER, Chain.AVALANCHE),
    (Protocol.RETRO, Chain.POLYGON),
    (Protocol.STELLASWAP, Chain.MOONBEAM),
    (Protocol.BEAMSWAP, Chain.MOONBEAM),
    (Protocol.SPIRITSWAP, Chain.FANTOM),
    (Protocol.SUSHI, Chain.POLYGON),
    (Protocol.SUSHI, Chain.ARBITRUM),
    (Protocol.SUSHI, Chain.BASE),
    (Protocol.RAMSES, Chain.ARBITRUM),
    (Protocol.ASCENT, Chain.POLYGON),
    (Protocol.FUSIONX, Chain.MANTLE),
    (Protocol.SYNTHSWAP, Chain.BASE),
    (Protocol.LYNEX, Chain.LINEA),
    (Protocol.PEGASYS, Chain.ROLLUX),
    (Protocol.BASEX, Chain.BASE),
    (Protocol.PANCAKESWAP, Chain.ARBITRUM),
    (Protocol.APERTURE, Chain.MANTA),
    (Protocol.QUICKSWAP, Chain.MANTA),
    (Protocol.HERCULES, Chain.METIS),
]


def build_routers() -> list:
    routes = []

    # all-deployments
    routes.append(
        subgraph_router_builder_allDeployments(
            tags=["All Deployments"], prefix="/allDeployments"
        )
    )

    # setup dex + chain endpoints
    for protocol, chain in DEPLOYED:
        routes.append(
            subgraph_router_builder(
                dex=protocol,
                chain=chain,
                tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                prefix=f"/{protocol.api_url}/{chain.api_url}",
            )
        )

    # Simulation
    routes.append(
        subgraph_router_builder_Simulator(tags=["Simulator"], prefix="/simulator")
    )

    # Charts
    routes.append(subgraph_router_builder_Charts(tags=["Charts"], prefix="/charts"))

    return routes


# manual
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
            chain=Chain.ETHEREUM,
            tags=["Mainnet"],
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.POLYGON,
            tags=["Polygon"],
            prefix=f"/{Chain.POLYGON.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.ARBITRUM,
            tags=["Arbitrum"],
            prefix=f"/{Chain.ARBITRUM.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.OPTIMISM,
            tags=["Optimism"],
            prefix=f"/{Chain.OPTIMISM.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.CELO,
            tags=["Celo"],
            prefix=f"/{Chain.CELO.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.BSC,
            tags=["BSC"],
            prefix=f"/{Chain.BSC.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.UNISWAP,
            chain=Chain.MOONBEAM,
            tags=["Moonbeam"],
            prefix=f"/{Chain.MOONBEAM.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.QUICKSWAP,
            chain=Chain.POLYGON,
            tags=["Quickswap - Polygon"],
            prefix=f"/{Protocol.QUICKSWAP.api_url}/{Chain.POLYGON.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.QUICKSWAP,
            chain=Chain.POLYGON_ZKEVM,
            tags=["Quickswap - Polygon zkEVM"],
            prefix=f"/{Protocol.QUICKSWAP.api_url}/{Chain.POLYGON_ZKEVM.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.ZYBERSWAP,
            chain=Chain.ARBITRUM,
            tags=["Zyberswap - Arbitrum"],
            prefix=f"/{Protocol.ZYBERSWAP.api_url}/{Chain.ARBITRUM.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.THENA,
            chain=Chain.BSC,
            tags=["Thena - BSC"],
            prefix=f"/{Protocol.THENA.api_url}/{Chain.BSC.api_url}",
        )
    )
    routes.append(
        subgraph_router_builder(
            dex=Protocol.CAMELOT,
            chain=Chain.ARBITRUM,
            tags=["Camelot - Arbitrum"],
            prefix=f"/{Protocol.CAMELOT.api_url}/{Chain.ARBITRUM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.GLACIER,
            chain=Chain.AVALANCHE,
            tags=["Glacier - Avalanche"],
            prefix=f"/{Protocol.GLACIER.api_url}/{Chain.AVALANCHE.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.RETRO,
            chain=Chain.POLYGON,
            tags=["Retro - Polygon"],
            prefix=f"/{Protocol.RETRO.api_url}/{Chain.POLYGON.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.STELLASWAP,
            chain=Chain.MOONBEAM,
            tags=["StellaSwap - Moonbeam"],
            prefix=f"/{Protocol.STELLASWAP.api_url}/{Chain.MOONBEAM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.BEAMSWAP,
            chain=Chain.MOONBEAM,
            tags=["BeamSwap - Moonbeam"],
            prefix=f"/{Protocol.BEAMSWAP.api_url}/{Chain.MOONBEAM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.SPIRITSWAP,
            chain=Chain.FANTOM,
            tags=["SpiritSwap - Fantom"],
            prefix=f"/{Protocol.SPIRITSWAP.api_url}/{Chain.FANTOM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.SUSHI,
            chain=Chain.POLYGON,
            tags=["Sushi - Polygon"],
            prefix=f"/{Protocol.SUSHI.api_url}/{Chain.POLYGON.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.SUSHI,
            chain=Chain.ARBITRUM,
            tags=["Sushi - Arbitrum"],
            prefix=f"/{Protocol.SUSHI.api_url}/{Chain.ARBITRUM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.SUSHI,
            chain=Chain.BASE,
            tags=["Sushi - Base"],
            prefix=f"/{Protocol.SUSHI.api_url}/{Chain.BASE.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.RAMSES,
            chain=Chain.ARBITRUM,
            tags=["Ramses - Arbitrum"],
            prefix=f"/{Protocol.RAMSES.api_url}/{Chain.ARBITRUM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.ASCENT,
            chain=Chain.POLYGON,
            tags=["Ascent - Polygon"],
            prefix=f"/{Protocol.ASCENT.api_url}/{Chain.POLYGON.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.FUSIONX,
            chain=Chain.MANTLE,
            tags=["FusionX - Mantle"],
            prefix=f"/{Protocol.FUSIONX.api_url}/{Chain.MANTLE.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.SYNTHSWAP,
            chain=Chain.BASE,
            tags=["Synthswap - Base"],
            prefix=f"/{Protocol.SYNTHSWAP.api_url}/{Chain.BASE.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.LYNEX,
            chain=Chain.LINEA,
            tags=["Lynex - Linea"],
            prefix=f"/{Protocol.LYNEX.api_url}/{Chain.LINEA.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.PEGASYS,
            chain=Chain.ROLLUX,
            tags=["Pegasys - Rollux"],
            prefix=f"/{Protocol.PEGASYS.api_url}/{Chain.ROLLUX.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.BASEX,
            chain=Chain.BASE,
            tags=["BaseX - Base"],
            prefix=f"/{Protocol.BASEX.api_url}/{Chain.BASE.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.PANCAKESWAP,
            chain=Chain.ARBITRUM,
            tags=["Pancakeswap - Arbitrum"],
            prefix=f"/{Protocol.PANCAKESWAP.api_url}/{Chain.ARBITRUM.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.APERTURE,
            chain=Chain.MANTA,
            tags=["Aperture - Manta"],
            prefix=f"/{Protocol.APERTURE.api_url}/{Chain.MANTA.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.QUICKSWAP,
            chain=Chain.MANTA,
            tags=["Quickswap - Manta"],
            prefix=f"/{Protocol.QUICKSWAP.api_url}/{Chain.MANTA.api_url}",
        )
    )

    routes.append(
        subgraph_router_builder(
            dex=Protocol.HERCULES,
            chain=Chain.METIS,
            tags=["Hercules - Metis"],
            prefix=f"/{Protocol.HERCULES.api_url}/{Chain.METIS.api_url}",
        )
    )

    # Simulation
    routes.append(
        subgraph_router_builder_Simulator(tags=["Simulator"], prefix="/simulator")
    )

    # Charts
    routes.append(subgraph_router_builder_Charts(tags=["Charts"], prefix="/charts"))

    return routes


# Route underlying functions


class subgraph_router_builder(router_builder_generalTemplate):
    def _create_routes(self, dex, chain) -> APIRouter:
        """Create routes for the given chain and dex combination."""

        router = APIRouter()

        # ROOT
        router.add_api_route(
            path=f"{self.prefix}/",
            endpoint=self.root,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # create special
        router = self._create_routes_special(router=router, dex=dex, chain=chain)

        # create all other routes
        router = self._create_routes_hypervisor(router=router, dex=dex, chain=chain)
        router = self._create_routes_hypervisor_analytics(router, dex, chain)

        router = self._create_routes_hypervisors(router, dex, chain)
        router = self._create_routes_hypervisors_rewards(router, dex, chain)

        router = self._create_routes_users_rewards(router, dex, chain)
        router = self._create_routes_users(router, dex, chain)

        router = self._create_routes_vault(router, dex, chain)

        return router

    def _create_routes_special(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/status/subgraph'}",
            endpoint=self.subgraph_status,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/charts/baseRange/all'}",
            endpoint=self.base_range_chart_all,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/charts/baseRange/{hypervisor_address}'}",
            endpoint=self.base_range_chart,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # create only on Mainnet
        if self.chain == Chain.ETHEREUM:
            router.add_api_route(
                path=f"{self.prefix}{'/charts/benchmark/{hypervisor_address}'}",
                endpoint=self.benchmark_chart,
                methods=["GET"],
                generate_unique_id_function=self.generate_unique_id,
            )

        return router

    def _create_routes_hypervisor_analytics(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/analytics/basic'}",
            endpoint=self.hypervisor_analytics_basic,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router = super()._create_routes_hypervisor_analytics(router, dex, chain)

        return router

    # EXECUTION FUNCTIONS

    async def subgraph_status(self, response: Response) -> SubgraphStatusOutput:
        return await subgraph_status(self.dex, self.chain)

    async def base_range_chart_all(self, response: Response, days: int = 20):
        return await charts.base_range_chart_all(self.dex, self.chain, days)

    async def base_range_chart(
        self, response: Response, hypervisor_address: str, days: int = 20
    ):
        return await charts.base_range_chart(
            self.dex, self.chain, hypervisor_address, days
        )

    async def benchmark_chart(
        self,
        response: Response,
        hypervisor_address: str,
        startDate: str = "",
        endDate: str = "",
    ):
        return await charts.benchmark_chart(
            self.dex, self.chain, hypervisor_address, startDate, endDate
        )

    async def hypervisor_basic_stats(self, hypervisor_address: str, response: Response):
        return await basic_stats_output(
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
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=1,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_weekly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=7,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_biweekly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=14,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_monthly(
        self, hypervisor_address: str, response: Response
    ):
        return await analytics.get_hype_data(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=30,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic(
        self, hypervisor_address: str, response: Response, period: Period
    ):
        return await analytics.get_hype_data(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=period.days,
        )

    #    hypervisors
    async def hypervisors_aggregate_stats(self, response: Response):
        result = aggregate_stats.AggregateStats(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await result.run(RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_returns(self, response: Response):
        """fee's Apr and Apy"""
        hypervisor_returns = hypervisor.HypervisorsReturnsAllPeriods(
            protocol=self.dex,
            chain=self.chain,
            hypervisors=None,
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
        usd_total_only: bool = False,
    ):
        """Retrieve collected fees for all hypervisors
        When default values are used, the function will return the last month's fees collected
        """
        return await hypervisor.collected_fees(
            protocol=self.dex,
            chain=self.chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
            usd_total_only=usd_total_only,
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

        return await masterchef_v2_info.run(
            QueryType.DATABASE
            if (self.dex, self.chain) in THIRD_PARTY_REWARDERS
            else RUN_FIRST
        )

    async def user_rewards(self, user_address: str, response: Response):
        return await masterchef.user_rewards(
            protocol=self.dex, chain=self.chain, user_address=user_address
        )

    async def user_rewards2(self, user_address: str, response: Response):
        if (self.dex, self.chain) in THIRD_PARTY_REWARDERS:
            # return database content
            return await masterchef_v2.user_rewards_thirdParty(
                protocol=self.dex, chain=self.chain, user_address=user_address.lower()
            )

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
        result = GammaInfo(Chain.ETHEREUM, days=30)
        return await result.output()

    async def gamma_yield(self, response: Response):
        result = GammaYield(Chain.ETHEREUM, days=30)
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
        # maximum subgraph query = 1000 items (in this case days)
        max_days_back = 1000

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

        # make sure days is not greater than max_days_back
        days = max_days_back if days > max_days_back else days

        # get the distribution class
        token_distributions = distribution_class_map[token_symbol](
            self.chain, days=days, timezone=timezone
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
                if deployment[0] != Protocol.GLACIER
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
        result = GammaInfo(Chain.ETHEREUM, days=30)
        return await result.output()

    async def gamma_yield(self, response: Response):
        result = GammaYield(Chain.ETHEREUM, days=30)
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
        tokens = await SimulatorInfo(Protocol.UNISWAP, Chain.ETHEREUM).token_list()

        return tokens

    async def pool_ticks(self, poolAddress: str):
        ticks = await SimulatorInfo(Protocol.UNISWAP, Chain.ETHEREUM).pool_ticks(
            poolAddress
        )

        return ticks

    async def pool_from_tokens(self, token0: str, token1: str):
        pools = await SimulatorInfo(Protocol.UNISWAP, Chain.ETHEREUM).pools_from_tokens(
            token0, token1
        )

        return pools

    async def pool_24hr_volume(self, poolAddress: str):
        volume = await SimulatorInfo(Protocol.UNISWAP, Chain.ETHEREUM).pool_volume(
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
    async def daily_tvl_chart_data(self, days: int = 24):
        daily = DailyChart(days)
        return {"data": await daily.tvl()}

    async def daily_flows_chart_data(self, days: int = 20):
        daily = DailyChart(days)
        return {"data": await daily.asset_flows()}

    async def daily_hypervisor_flows_chart_data(
        self, hypervisor_address: str, days: int = 20
    ):
        daily = DailyChart(days)
        return {"data": await daily.asset_flows(hypervisor_address)}
