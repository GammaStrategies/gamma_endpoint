import asyncio
import re
import typing
from fastapi import Query, Response, APIRouter, status
from fastapi.routing import APIRoute
from fastapi_cache.decorator import cache
from endpoint.config.cache import DB_CACHE_TIMEOUT

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.general.enums import Chain, Protocol
from sources.mongo.bins.apps import hypervisor
from sources.mongo.bins.apps import user
from sources.mongo.bins.apps import prices


DEPLOYED: list[tuple[Protocol, Chain]] = [
    (Protocol.UNISWAP, Chain.ETHEREUM),
    (Protocol.UNISWAP, Chain.POLYGON),
    (Protocol.UNISWAP, Chain.ARBITRUM),
    (Protocol.UNISWAP, Chain.OPTIMISM),
    (Protocol.UNISWAP, Chain.CELO),
    (Protocol.UNISWAP, Chain.BSC),
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
]


def build_routers() -> list:
    routes = []

    # setup protocol + chain endpoints
    for protocol, chain in DEPLOYED:
        routes.append(
            mongo_router_builder(
                protocol=protocol,
                chain=chain,
                tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                prefix=f"/{protocol.api_url}/{chain.api_url}",
            )
        )

    return routes


class mongo_router_builder(router_builder_baseTemplate):
    def __init__(
        self,
        protocol: Protocol,
        chain: Chain,
        tags: list | None = None,
        prefix: str = "",
    ):
        super().__init__(tags=tags, prefix=prefix)

        self.protocol = protocol
        self.chain = chain
        # set tags if not supplied
        self.tags = self.tags or [f"{chain.fantasy_name} - {protocol.fantasy_name}"]
        self.name = type(self).__name__

    def generate_unique_id(self, route: "APIRoute") -> str:
        operation_id = f"{self.name}_{self.tags}_{route.name + route.path_format}"
        operation_id = re.sub(r"\W", "_", operation_id)
        assert route.methods
        operation_id = operation_id + "_" + list(route.methods)[0].lower()
        return operation_id

    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return self._create_routes()

    def _create_routes(self) -> APIRouter:
        """Create routes for the given chain and protocol combination."""

        router = APIRouter()

        # ROOT
        router.add_api_route(
            path=f"{self.prefix}/",
            endpoint=self.root,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # create all other routes
        router = self._create_routes_hypervisor(router=router)

        router = self._create_routes_hypervisors(router=router)

        router = self._create_routes_user(router=router)

        router.add_api_route(
            path=f"{self.prefix}{'/prices'}",
            endpoint=self.prices,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisor(
        self,
        router: APIRouter,
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
        self,
        router: APIRouter,
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

    def _create_routes_user(self, router: APIRouter) -> APIRouter:
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
        return f"Gamma Strategies on {self.chain.fantasy_name}'s {self.protocol.fantasy_name} "

    # Hypervisor

    @cache(expire=DB_CACHE_TIMEOUT)
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

    @cache(expire=DB_CACHE_TIMEOUT)
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

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_prices(
        self,
        hypervisor_address: str,
        response: Response,
    ):
        """ """
        return await hypervisor.get_hypervisor_prices(
            network=self.chain, hypervisor_address=hypervisor_address
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def prices(
        self,
        response: Response,
        token_addresses: typing.List[str] = Query(None),
        block: int | None = None,
    ):
        """ """
        if token_addresses is None:
            return "Please provide a list of token addresses"
        return await prices.get_prices(
            token_addresses=[x.lower().strip() for x in token_addresses],
            network=self.chain,
            block=block,
        )

    # Hypervisors
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_list(self, response: Response):
        """Returns the hypervisor found in the database"""
        return await hypervisor.hypervisors_list(
            network=self.chain, protocol=self.protocol
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.hypervisors_uncollected_fees(network=self.chain)

    @cache(expire=DB_CACHE_TIMEOUT)
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
    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_data(self, address: str, response: Response):
        return await user.get_user_historic_info(chain=self.chain, address=address)

    @cache(expire=DB_CACHE_TIMEOUT)
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
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_basic_stats(self, hypervisor_address: str, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_returns(self, hypervisor_address: str, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_average_returns(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
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

    @cache(expire=DB_CACHE_TIMEOUT)
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

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_prices(
        self,
        hypervisor_address: str,
        response: Response,
    ):
        """ """
        return await hypervisor.get_hypervisor_prices(
            network=self.chain, hypervisor_address=hypervisor_address
        )

    #    hypervisor analytics
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_daily(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_weekly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_biweekly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_analytics_basic_monthly(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    #    hypervisors
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_list(self, response: Response):
        """Returns the hypervisor found in the database"""
        return await hypervisor.hypervisors_list(
            network=self.chain, protocol=self.protocol
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_aggregate_stats(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_returns(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_average_returns(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_all_data(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.hypervisors_uncollected_fees(network=self.chain)

    @cache(expire=DB_CACHE_TIMEOUT)
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

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_daily(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_weekly(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_feeReturns_monthly(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_daily(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_weekly(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_impermanentDivergence_monthly(self, response: Response):
        return "Not implemented yet"

    # others
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_rewards(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_rewards2(self, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_rewards(self, user_address: str, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_rewards2(self, user_address: str, response: Response):
        return "Not implemented yet"

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_data(self, address: str, response: Response):
        return await user.get_user_historic_info(chain=self.chain, address=address)

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_analytics(self, address: str, response: Response):
        return await user.get_user_analytic_data(chain=self.chain, address=address)

    @cache(expire=DB_CACHE_TIMEOUT)
    async def vault_data(self, address: str, response: Response):
        return "Not implemented yet"
