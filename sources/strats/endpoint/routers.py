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
from sources.strats.bins.apps import prices


def build_routers() -> list:
    routes = []

    # add global route
    routes.append(strats_router_builder_general(tags=[f" strats "]))

    return routes


class strats_router_builder_general(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return self._create_routes()

    def _create_routes(self) -> APIRouter:
        """Create routes for the given chain and protocol combination."""

        router = APIRouter()

        # ROOT
        router.add_api_route(
            path=f"/current_prices",
            endpoint=self.get_current_prices,
            methods=["GET"],
        )
        router.add_api_route(
            path=f"/token_list",
            endpoint=self.get_current_tokens_list,
            methods=["GET"],
        )

        return router

    # EXECUTION FUNCTIONS
    async def get_current_prices(
        self,
        response: Response,
        chain: Chain | None = Query(None, description="Chain to query"),
        token_addresses: list[str]
        | None = Query(None, description="Token addresses to query"),
    ) -> list[dict]:
        """Get a list of Gamma's chain token prices from database, updated in less than 10 minutes"""
        return await prices.get_current_prices(
            chain=chain, token_addresses=token_addresses
        )

    async def get_current_tokens_list(
        self,
        response: Response,
        chain: Chain | None = Query(None, description="Chain to query"),
    ) -> list[list]:
        """Get a comma separated list of Gamma's chain token addresses whos price is updated in less than 10 minutes"""
        return await prices.get_current_token_addresses(
            chain=chain,
        )
