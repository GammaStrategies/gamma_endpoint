import asyncio
import re
from fastapi import Response, APIRouter, status, Query
from fastapi.routing import APIRoute
from fastapi_cache.decorator import cache
from endpoint.routers.template import router_builder_baseTemplate

import typing

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


class web3_router_builder(router_builder_baseTemplate):
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

        router = self._create_routes_hypervisors(router, dex, chain)

        return router

    def _create_routes_hypervisor(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        """Create /hypervisor routes for the given chain and dex combination."""

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/uncollectedFees'}",
            endpoint=self.hypervisor_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/rewards'}",
            endpoint=self.hypervisor_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}'}",
            endpoint=self.hypervisor_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors(
        self, router: APIRouter, dex: str, chain: str
    ) -> APIRouter:
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors'}",
            endpoint=self.hypervisors_list,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS

    def root(self) -> str:
        return f"Gamma Strategies on {self.chain}'s {self.dex} "

    async def hypervisor_uncollected_fees(
        self, hypervisor_address: str, response: Response
    ):
        return await hypervisors.hypervisor_uncollected_fees(
            network=self.chain, dex=self.dex, hypervisor_address=hypervisor_address
        )

    async def hypervisor_rewards(
        self, hypervisor_address: str, response: Response, block: int | None = None
    ):
        """Rewards for a given hypervisor at a given block. If block is not supplied, it will return the latest block"""

        if self.dex in [Dex.THENA, Dex.ZYBERSWAP]:
            return await rewards.get_rewards(
                dex=self.dex,
                hypervisor_address=hypervisor_address,
                network=self.chain,
                block=block,
            )
        else:
            return " Not implemented yet"

    #    hypervisors

    async def hypervisors_list(self, response: Response):
        """Returns a list of low case hypervisor addresses found in registry"""
        return await hypervisors.hypervisors_list(network=self.chain, dex=self.dex)

    async def hypervisor_data(
        self,
        hypervisor_address: str,
        response: Response,
        fields: typing.List[str] = Query(None),
        block: int | None = None,
    ):
        """Given a contract function list [fields] returns the data for the given hypervisor address at the given block, if supplied
        By default returns decimals, totalSupply and getTotalAmounts.
        * To get **addresses**, specify so like:  pool.address or pool.token0.address
        * To get a **full dict** object, specify 'as_dict' like: pool.as_dict  or as_dict ( if you want to get all the hype data, including the pool)
        """
        return await hypervisors.get_hypervisor_data(
            network=self.chain,
            dex=self.dex,
            hypervisor_address=hypervisor_address,
            fields=fields or ["decimals", "totalSupply", "getTotalAmounts"],
            block=block,
        )
