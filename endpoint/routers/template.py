import re
from fastapi import APIRouter, Response, status
from fastapi_cache.decorator import cache
from fastapi.routing import APIRoute

from endpoint.config.cache import (
    ALLDATA_CACHE_TIMEOUT,
    APY_CACHE_TIMEOUT,
    DASHBOARD_CACHE_TIMEOUT,
    DB_CACHE_TIMEOUT,
)


class router_builder_baseTemplate:
    def __init__(self, tags: list, prefix: str = ""):
        self.tags = tags
        self.prefix = prefix.removesuffix("/")

    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return None


class router_builder_generalTemplate(router_builder_baseTemplate):
    def __init__(
        self, dex: str, chain: str, tags: list | None = None, prefix: str = ""
    ):
        super().__init__(tags=tags, prefix=prefix)

        self.dex = dex
        self.chain = chain
        # set tags if not supplied
        self.tags = self.tags or [f"{chain} - {dex}"]
        self.name = type(self).__name__

    def generate_unique_id(self, route: "APIRoute") -> str:
        operation_id = f"{self.name}_{self.tags}_{route.name + route.path_format}"
        operation_id = re.sub(r"\W", "_", operation_id)
        assert route.methods
        operation_id = operation_id + "_" + list(route.methods)[0].lower()
        return operation_id

    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return self._create_routes(dex=self.dex, chain=self.chain)

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

        # create all other routes
        router = self._create_routes_hypervisor(router=router, dex=dex, chain=chain)
        router = self._create_routes_hypervisor_analytics(router, dex, chain)

        router = self._create_routes_hypervisors(router, dex, chain)
        router = self._create_routes_hypervisors_rewards(router, dex, chain)

        router = self._create_routes_users_rewards(router, dex, chain)
        router = self._create_routes_users(router, dex, chain)

        router = self._create_routes_vault(router, dex, chain)

        return router

    def _create_routes_hypervisor(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        """Create /hypervisor routes for the given chain and dex combination."""

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/basicStats'}",
            endpoint=self.hypervisor_basic_stats,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/returns'}",
            endpoint=self.hypervisor_returns,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/averageReturns'}",
            endpoint=self.hypervisor_average_returns,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/uncollectedFees'}",
            endpoint=self.hypervisor_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        
        return router

    def _create_routes_hypervisor_analytics(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/analytics/basic/daily'}",
            endpoint=self.hypervisor_analytics_basic_daily,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/analytics/basic/weekly'}",
            endpoint=self.hypervisor_analytics_basic_weekly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/analytics/basic/biweekly'}",
            endpoint=self.hypervisor_analytics_basic_biweekly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/analytics/basic/monthly'}",
            endpoint=self.hypervisor_analytics_basic_monthly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/aggregateStats'}",
            endpoint=self.hypervisors_aggregate_stats,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/recentFees'}",
            endpoint=self.hypervisors_recent_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/returns'}",
            endpoint=self.hypervisors_returns,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/averageReturns'}",
            endpoint=self.hypervisors_average_returns,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
            description="Returns the average returns for all hypervisors.",
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/allData'}",
            endpoint=self.hypervisors_all_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/uncollectedFees'}",
            endpoint=self.hypervisors_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/collectedFees'}",
            endpoint=self.hypervisors_collected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/feeReturns/daily'}",
            endpoint=self.hypervisors_feeReturns_daily,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/feeReturns/weekly'}",
            endpoint=self.hypervisors_feeReturns_weekly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/feeReturns/monthly'}",
            endpoint=self.hypervisors_feeReturns_monthly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/impermanentDivergence/daily'}",
            endpoint=self.hypervisors_impermanentDivergence_daily,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/impermanentDivergence/weekly'}",
            endpoint=self.hypervisors_impermanentDivergence_weekly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/impermanentDivergence/monthly'}",
            endpoint=self.hypervisors_impermanentDivergence_monthly,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors_rewards(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/allRewards'}",
            endpoint=self.hypervisors_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/allRewards2'}",
            endpoint=self.hypervisors_rewards2,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_users_rewards(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/userRewards/{user_address}'}",
            endpoint=self.user_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/userRewards2/{user_address}'}",
            endpoint=self.user_rewards2,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_users(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/user/{address}'}",
            endpoint=self.user_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )
        router.add_api_route(
            path=f"{self.prefix}{'/user/{address}/analytics'}",
            endpoint=self.user_analytics,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_vault(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/vault/{address}'}",
            endpoint=self.vault_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS
    def root(self) -> str:
        return f"Gamma Strategies on {self.chain}'s {self.dex} "

    async def hypervisor_basic_stats(self, hypervisor_address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_returns(self, hypervisor_address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_average_returns(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_uncollected_fees(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    #    hypervisor analytics
    async def hypervisor_analytics_basic_daily(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_analytics_basic_weekly(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_analytics_basic_biweekly(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisor_analytics_basic_monthly(
        self, hypervisor_address: str, response: Response
    ):
        return NotImplementedError(" function defaults not implemented yet")

    #    hypervisors
    async def hypervisors_aggregate_stats(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_recent_fees(self, response: Response, hours: int = 24):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_returns(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_average_returns(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_all_data(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_uncollected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_feeReturns_daily(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_feeReturns_weekly(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_feeReturns_monthly(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_impermanentDivergence_daily(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_impermanentDivergence_weekly(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_impermanentDivergence_monthly(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    # rewards
    # TODO: one only function for hype rewards
    async def hypervisors_rewards(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def hypervisors_rewards2(self, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    # TODO: one only function for user rewards
    async def user_rewards(self, user_address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def user_rewards2(self, user_address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def user_data(self, address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def user_analytics(self, address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")

    async def vault_data(self, address: str, response: Response):
        return NotImplementedError(" function defaults not implemented yet")
