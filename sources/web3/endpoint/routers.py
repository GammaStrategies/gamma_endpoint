import asyncio
import re
from fastapi import Response, APIRouter, status, Query
from fastapi.routing import APIRoute
from fastapi_cache.decorator import cache
from endpoint.routers.template import router_builder_baseTemplate

import typing

from sources.common.general.enums import Dex, Chain, Protocol

from sources.web3.bins.apps import hypervisors, rewards

# Route builders

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
    (Protocol.SUSHI, Chain.ARBITRUM),
    (Protocol.RAMSES, Chain.ARBITRUM),
    (Protocol.ASCENT, Chain.POLYGON),
    (Protocol.FUSIONX, Chain.MANTLE),
    (Protocol.SYNTHSWAP, Chain.BASE),
    (Protocol.LYNEX, Chain.LINEA)
]


def build_routers() -> list:
    routes = []

    # setup dex + chain endpoints
    for protocol, chain in DEPLOYED:
        routes.append(
            web3_router_builder(
                dex=protocol,
                chain=chain,
                tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                prefix=f"/{protocol.api_url}/{chain.api_url}",
            )
        )

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
