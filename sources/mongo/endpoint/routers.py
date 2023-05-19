import asyncio
import re
from fastapi import Response, APIRouter, status
from fastapi.routing import APIRoute
from fastapi_cache.decorator import cache

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.general.enums import Dex, Chain
from sources.mongo.bins.apps import hypervisor
from sources.mongo.bins.apps import user


def build_routers() -> list:
    routes = []

    # all-deployments
    # TODO: add all-deployments route

    # setup dex + chain endpoints

    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.ETHEREUM,
            tags=["Uniswap - Ethereum"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.ETHEREUM.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.POLYGON,
            tags=["Uniswap - Polygon"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.ARBITRUM,
            tags=["Uniswap - Arbitrum"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.OPTIMISM,
            tags=["Uniswap - Optimism"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.OPTIMISM.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.CELO,
            tags=["Uniswap - Celo"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.CELO.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.BSC,
            tags=["Uniswap - Binance"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.BSC.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.QUICKSWAP,
            chain=Chain.POLYGON,
            tags=["Quickswap - Polygon"],
            prefix=f"/{Dex.QUICKSWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.ZYBERSWAP,
            chain=Chain.ARBITRUM,
            tags=["Zyberswap - Arbitrum"],
            prefix=f"/{Dex.ZYBERSWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        mongo_router_builder(
            dex=Dex.THENA,
            chain=Chain.BSC,
            tags=["Thena - BSC"],
            prefix=f"/{Dex.THENA.value}/{Chain.BSC.value}",
        )
    )

    # Simulation
    # TODO: add simulation route

    # Charts
    # TODO: add charts route

    return routes


class mongo_router_builder(router_builder_baseTemplate):
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

        router = self._create_routes_hypervisors(router=router, dex=dex, chain=chain)

        router = self._create_routes_user(router=router, dex=dex, chain=chain)

        return router

    def _create_routes_hypervisor(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        """Create /hypervisor routes for the given chain and dex combination."""

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/collectedFees'}",
            endpoint=self.hypervisor_collected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/uncollectedFees'}",
            endpoint=self.hypervisor_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/prices'}",
            endpoint=self.hypervisor_prices,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        # add hypervisor list
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors'}",
            endpoint=self.hypervisors_list,
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
            path=f"{self.prefix}{'/hypervisors/uncollectedFees'}",
            endpoint=self.hypervisors_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_user(self, router: APIRouter, dex: str, chain: str) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/user/{user_address}'}",
            endpoint=self.user_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/user/{user_address}/analytics'}",
            endpoint=self.user_analytics,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS

    def root(self) -> str:
        return f"Gamma Strategies on {self.chain}'s {self.dex} "

    # Hypervisor

    async def hypervisor_uncollected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        timestamp: int | None = None,
        block: int | None = None,
    ):
        return await hypervisor.hypervisor_uncollected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            timestamp=timestamp,
            block=block,
        )

    async def hypervisor_collected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisor_collected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    async def hypervisor_prices(
        self,
        hypervisor_address: str,
        response: Response,
    ):
        """ """
        return await hypervisor.get_hypervisor_prices(
            network=self.chain, dex=self.dex, hypervisor_address=hypervisor_address
        )

    # Hypervisors

    async def hypervisors_list(self, response: Response):
        """Returns the hypervisor found in the database"""
        return await hypervisor.hypervisors_list(network=self.chain, dex=self.dex)

    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.hypervisors_uncollected_fees(network=self.chain)

    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisors_collected_fees(
            network=self.chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    # User

    async def user_data(self, address: str, response: Response):
        return await user.get_user_historic_info(chain=self.chain, address=address)

    async def user_analytics(self, address: str, response: Response):
        return await user.get_user_analytic_data(chain=self.chain, address=address)


class mongo_router_builder_compatible(router_builder_generalTemplate):
    # ROUTEs BUILD FUNCTIONS

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

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/collectedFees'}",
            endpoint=self.hypervisor_collected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/prices'}",
            endpoint=self.hypervisor_prices,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        # add hypervisor list
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors'}",
            endpoint=self.hypervisors_list,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # add inherited default routes
        router = super()._create_routes_hypervisors(router, dex, chain)

        # add hype rewards
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/rewards'}",
            endpoint=self.hypervisors_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/rewards2'}",
            endpoint=self.hypervisors_rewards2,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS

    async def hypervisor_basic_stats(self, hypervisor_address: str, response: Response):
        return "Not implemented yet"

    async def hypervisor_returns(self, hypervisor_address: str, response: Response):
        return "Not implemented yet"

    async def hypervisor_average_returns(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    async def hypervisor_uncollected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        timestamp: int | None = None,
        block: int | None = None,
    ):
        return await hypervisor.hypervisor_uncollected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            timestamp=timestamp,
            block=block,
        )

    async def hypervisor_collected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisor_collected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    async def hypervisor_prices(
        self,
        hypervisor_address: str,
        response: Response,
    ):
        """ """
        return await hypervisor.get_hypervisor_prices(
            network=self.chain, dex=self.dex, hypervisor_address=hypervisor_address
        )

    #    hypervisor analytics

    async def hypervisor_analytics_basic_daily(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    async def hypervisor_analytics_basic_weekly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    async def hypervisor_analytics_basic_biweekly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    async def hypervisor_analytics_basic_monthly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    #    hypervisors

    async def hypervisors_list(self, response: Response):
        """Returns the hypervisor found in the database"""
        return await hypervisor.hypervisors_list(network=self.chain, dex=self.dex)

    async def hypervisors_aggregate_stats(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_returns(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_average_returns(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_all_data(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.hypervisors_uncollected_fees(network=self.chain)

    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisors_collected_fees(
            network=self.chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    async def hypervisors_feeReturns_daily(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_feeReturns_weekly(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_feeReturns_monthly(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_impermanentDivergence_daily(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_impermanentDivergence_weekly(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_impermanentDivergence_monthly(self, response: Response):
        return "Not implemented yet"

    # others
    async def hypervisors_rewards(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_rewards2(self, response: Response):
        return "Not implemented yet"

    async def user_rewards(self, user_address: str, response: Response):
        return "Not implemented yet"

    async def user_rewards2(self, user_address: str, response: Response):
        return "Not implemented yet"

    async def user_data(self, address: str, response: Response):
        return await user.get_user_historic_info(chain=self.chain, address=address)

    async def user_analytics(self, address: str, response: Response):
        return await user.get_user_analytic_data(chain=self.chain, address=address)

    async def vault_data(self, address: str, response: Response):
        return "Not implemented yet"
