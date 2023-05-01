import asyncio
from fastapi import Response, APIRouter, status
from fastapi_cache.decorator import cache

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.general.enums import Dex, Chain

from sources.web3.bins.apps import hypervisors, rewards

# Route builders


def build_routers() -> list:
    routes = []

    # all-deployments
    # TODO: add all-deployments route

    # setup dex + chain endpoints

    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.ETHEREUM,
            tags=["Uniswap - Ethereum"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.ETHEREUM.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.POLYGON,
            tags=["Uniswap - Polygon"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.ARBITRUM,
            tags=["Uniswap - Arbitrum"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.OPTIMISM,
            tags=["Uniswap - Optimism"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.OPTIMISM.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.CELO,
            tags=["Uniswap - Celo"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.CELO.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.UNISWAP,
            chain=Chain.BSC,
            tags=["Uniswap - Binance"],
            prefix=f"/{Dex.UNISWAP.value}/{Chain.BSC.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.QUICKSWAP,
            chain=Chain.POLYGON,
            tags=["Quickswap - Polygon"],
            prefix=f"/{Dex.QUICKSWAP.value}/{Chain.POLYGON.value}",
        )
    )
    routes.append(
        web3_router_builder(
            dex=Dex.ZYBERSWAP,
            chain=Chain.ARBITRUM,
            tags=["Zyberswap - Arbitrum"],
            prefix=f"/{Dex.ZYBERSWAP.value}/{Chain.ARBITRUM.value}",
        )
    )
    routes.append(
        web3_router_builder(
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


# Route underlying functions


class web3_router_builder(router_builder_generalTemplate):
    # ROUTEs BUILD FUNCTIONS

    def _create_routes_hypervisor(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        """Create /hypervisor routes for the given chain and dex combination."""

        router = super()._create_routes_hypervisor(router=router, dex=dex, chain=chain)

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/rewards'}",
            endpoint=self.hypervisor_rewards,
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

    async def hypervisor_collected_fees(
        self, hypervisor_address: str, response: Response
    ):
        return "Not implemented yet"

    async def hypervisor_uncollected_fees(
        self, hypervisor_address: str, response: Response
    ):
        return await hypervisors.hypervisor_uncollected_fees(
            network=self.chain, dex=self.dex, hypervisor_address=hypervisor_address
        )

    async def hypervisor_rewards(self, hypervisor_address: str, response: Response):
        return await rewards.get_rewards(
            dex=self.dex, hypervisor_address=hypervisor_address, network=self.chain
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
        """Returns a list of low case hypervisor addresses found in registry"""
        return await hypervisors.hypervisors_list(network=self.chain, dex=self.dex)

    async def hypervisors_aggregate_stats(self, response: Response):
        return "Not implemented yet"

    async def hypervisors_recent_fees(self, response: Response, hours: int = 24):
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
        return "Not implemented yet"

    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return "Not implemented yet"

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
        return "Not implemented yet"

    async def user_analytics(self, address: str, response: Response):
        return "Not implemented yet"

    async def vault_data(self, address: str, response: Response):
        return "Not implemented yet"


# class web3_special_builder(router_builder_baseTemplate):


#     # ROUTEs BUILD FUNCTIONS
#     def router(self) -> APIRouter:
#         router = APIRouter(prefix=self.prefix)

#         #
#         router.add_api_route(
#             path="/hypervisors/aggregateStats",
#             endpoint=self.aggregate_stats,
#             methods=["GET"],
#         )
