import asyncio
from datetime import datetime, timezone
import logging
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi_cache.decorator import cache

from endpoint.config.cache import (
    DAILY_CACHE_TIMEOUT,
)
from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.frontend.bins.revenue_stats import get_revenue_stats

from sources.subgraph.bins.enums import Chain, Protocol


# Route builders


def build_routers() -> list:
    routes = []

    routes.append(
        frontend_router_builder_main(tags=["Frontend endpoints"], prefix="/frontend")
    )

    return routes


# Route underlying functions


class frontend_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/revenue_status",
            endpoint=self.revenue_status,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DAILY_CACHE_TIMEOUT)
    async def revenue_status(
        self,
        response: Response,
        chain: Chain | None = None,
        protocol: Protocol | None = None,
        from_timestamp: int | None = None,
        yearly: bool = False,
    ) -> list[dict]:
        """Returns Gamma's fees aquired by hypervisors, calculated volume of swaps on those same hypervisors and their revenue (Gamma service fees).

        * **from_timestamp** Limit returned data from timestamp to now.
        * **yearly** group result by year.

        """

        return await get_revenue_stats(
            chain=chain, protocol=protocol, yearly=yearly, ini_timestamp=from_timestamp
        )
