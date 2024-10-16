import asyncio
from fastapi import APIRouter, Response, status, Query
from fastapi_cache.decorator import cache

from endpoint.config.cache import (
    ALLDATA_CACHE_TIMEOUT,
    APY_CACHE_TIMEOUT,
    CHARTS_CACHE_TIMEOUT,
    DASHBOARD_CACHE_TIMEOUT,
    DB_CACHE_TIMEOUT,
    USER_CACHE_TIMEOUT,
)
from endpoint.routers.template import (
    router_builder_baseTemplate,
    router_builder_generalTemplate,
)
from endpoint.utilities import add_deprecated_message
from sources.common.general.enums import Period, int_to_chain
from sources.subgraph.bins.charts.daily import DailyChart
from sources.subgraph.bins.common import (
    SubgraphStatusOutput,
    aggregate_stats as agg_stats,
    analytics,
    hypervisor,
    masterchef,
    masterchef_v2,
    recovery,
    subgraph_status,
    users,
)
from sources.subgraph.bins.common.hypervisor import (
    hypervisor_basic_stats as basic_stats_output,
    unified_hypervisors_data,
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
    for protocol, chain in DEPLOYMENTS:
        routes.append(
            subgraph_router_builder(
                dex=protocol,
                chain=chain,
                tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                prefix=f"/{protocol.api_url}/{chain.api_url}",
            )
        )

    # Charts
    routes.append(subgraph_router_builder_Charts(tags=["Charts"], prefix="/charts"))

    return routes


def build_routers_compatible() -> list:
    routes = []

    # all-deployments
    routes.append(
        subgraph_router_builder_allDeployments(
            tags=["All Deployments"], prefix="/allDeployments"
        )
    )

    # setup dex + chain endpoints
    for protocol, chain in DEPLOYMENTS:
        if protocol == Protocol.UNISWAP:
            if chain == Chain.ETHEREUM:
                routes.append(
                    subgraph_router_builder_compatible(
                        dex=protocol,
                        chain=chain,
                        tags=["Mainnet"],
                    )
                )
            else:
                routes.append(
                    subgraph_router_builder(
                        dex=protocol,
                        chain=chain,
                        tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                        prefix=f"/{chain.api_url}",
                    )
                )
        else:
            routes.append(
                subgraph_router_builder(
                    dex=protocol,
                    chain=chain,
                    tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                    prefix=f"/{protocol.api_url}/{chain.api_url}",
                )
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

        if self.chain == Chain.ARBITRUM:
            router.add_api_route(
                path=f"{self.prefix}{'/recoveryDistribution'}",
                endpoint=self.recovery_stats,
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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

        return await analytics.get_hype_data(
            protocol=self.dex,
            chain=self.chain,
            hypervisor_address=hypervisor_address,
            period=period.days,
        )

    #    hypervisors
    @cache(expire=ALLDATA_CACHE_TIMEOUT)
    async def hypervisors_aggregate_stats(self, response: Response):
        result = agg_stats.AggregateStats(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await result.run(RUN_FIRST)

    @cache(expire=ALLDATA_CACHE_TIMEOUT)
    async def hypervisors_basic_stats(self, response: Response):
        all_data = hypervisor.HypeBasicStats(
            protocol=self.dex, chain=self.chain, response=response
        )
        return await all_data.run(RUN_FIRST)

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

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.uncollected_fees_all(
            protocol=self.dex,
            chain=self.chain,
        )

    @cache(expire=APY_CACHE_TIMEOUT)
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
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=1, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_weekly(self, response: Response):
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=7, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    @cache(expire=APY_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_monthly(self, response: Response):
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

        impermanent = hypervisor.ImpermanentDivergence(
            protocol=self.dex, chain=self.chain, days=30, response=response
        )
        return await impermanent.run(first=RUN_FIRST)

    # others
    @cache(expire=ALLDATA_CACHE_TIMEOUT)
    async def hypervisors_rewards(self, response: Response):
        return await masterchef.info(protocol=self.dex, chain=self.chain)

    @cache(expire=ALLDATA_CACHE_TIMEOUT)
    async def hypervisors_rewards2(self, response: Response):
        masterchef_v2_info = masterchef_v2.AllRewards2(
            protocol=self.dex, chain=self.chain, response=response
        )

        return await masterchef_v2_info.run(
            QueryType.DATABASE
            if (self.dex, self.chain) in THIRD_PARTY_REWARDERS
            else RUN_FIRST
        )

    @cache(expire=USER_CACHE_TIMEOUT)
    async def user_rewards(self, user_address: str, response: Response):
        return await masterchef.user_rewards(
            protocol=self.dex, chain=self.chain, user_address=user_address
        )

    @cache(expire=USER_CACHE_TIMEOUT)
    async def user_rewards2(self, user_address: str, response: Response):
        if (self.dex, self.chain) in THIRD_PARTY_REWARDERS:
            # return database content
            return await masterchef_v2.user_rewards_thirdParty(
                protocol=self.dex, chain=self.chain, user_address=user_address.lower()
            )

        return await masterchef_v2.user_rewards(
            protocol=self.dex, chain=self.chain, user_address=user_address
        )

    @cache(expire=USER_CACHE_TIMEOUT)
    async def user_data(self, address: str, response: Response):
        return await users.user_data(
            protocol=self.dex, chain=self.chain, address=address
        )

    @cache(expire=USER_CACHE_TIMEOUT)
    async def user_analytics(self, address: str, response: Response):
        ## DEPRECATED
        message = "This endpoint is no longer valid."
        response = add_deprecated_message(response, message=message)
        return [message]

        return await users.get_user_analytic_data(
            chain=self.chain, address=address.lower()
        )

    @cache(expire=USER_CACHE_TIMEOUT)
    async def vault_data(self, address: str, response: Response):
        return await users.account_data(
            protocol=self.dex, chain=self.chain, address=address
        )

    @cache(expire=APY_CACHE_TIMEOUT)
    async def recovery_stats(self, days: int = 7, timezone: str = "UTC-5"):
        return await recovery.recovery_stats(days, timezone)


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

        router.add_api_route(
            path="/unifiedHypervisorsData",
            endpoint=self.unifiedHypervisorsData,
            methods=["GET"],
        )

        return router

    @cache(expire=DASHBOARD_CACHE_TIMEOUT)
    async def aggregate_stats(
        self,
        response: Response,
    ) -> agg_stats.AggregateStatsDeploymentInfoOutput:
        results = await asyncio.gather(
            *{
                agg_stats.AggregateStats(deployment[0], deployment[1], response).run(
                    RUN_FIRST
                )
                for deployment in DEPLOYMENTS
                if deployment
                not in [
                    (Protocol.GLACIER, Chain.AVALANCHE),
                    (Protocol.THENA, Chain.OPBNB),
                    (Protocol.KODIAK, Chain.BARTIO),
                ]
            },
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

        return agg_stats.AggregateStatsDeploymentInfoOutput(
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

    @cache(expire=DB_CACHE_TIMEOUT)
    async def unifiedHypervisorsData(
        self,
        response: Response,
        chain: Chain | int = Query(
            None,
            enum=[*Chain, *[x.id for x in Chain]],
            description="Chain to filter by. When None, it will return all chains.",
        ),
        protocol: Protocol = Query(
            None,
            enum=[*Protocol],
            description="Protocol to filter by. When None, it will return all protocols.",
        ),
    ):
        # convert
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        # database call only
        return await unified_hypervisors_data(chain=chain, protocol=protocol)


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
