import asyncio
from datetime import datetime, timezone
import logging
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi_cache.decorator import cache

from endpoint.config.cache import (
    DAILY_CACHE_TIMEOUT,
    DB_CACHE_TIMEOUT,
    LONG_CACHE_TIMEOUT,
)
from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.general.utils import filter_addresses
from sources.frontend.bins.analytics import get_positions_analysis
from sources.frontend.bins.correlation import (
    get_correlation,
    get_correlation_from_hypervisors,
)
from sources.frontend.bins.revenue_stats import get_revenue_stats

from sources.subgraph.bins.enums import Chain, Protocol


# Route builders


def build_routers() -> list:
    routes = []

    routes.append(
        frontend_revenueStatus_router_builder_main(tags=["Revenue status"], prefix="")
    )
    routes.append(frontend_analytics_router_builder_main(tags=["Analytics"], prefix=""))

    return routes


# Route underlying functions


class frontend_revenueStatus_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/revenue_status/main_charts",
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
        filter_zero_revenue: bool = True,
    ) -> list[dict]:
        """Returns Gamma's fees aquired by hypervisors, calculated volume of swaps on those same hypervisors and their revenue (Gamma service fees).

        * **total_revenue** are all tokens transfered to Gamma's fee accounts from hypervisors and other sources (line veNFT). USD token prices are from the date the transfer happened.
        * **total_fees** are all fees aquired by the hypervisors.  USD token prices are from the date it happened but can contain some posterior prices (week).
        * **total_volume** is calculated using **total_fees**.

        ### Query parameters
        * **chain** Chain to filter by.
        * **protocol** Protocol to filter by.
        * **from_timestamp** Limit returned data from timestamp to now.
        * **yearly** group result by year.
        * **filter_zero_revenue** filter out zero revenue items.

        """

        return await get_revenue_stats(
            chain=chain,
            protocol=protocol,
            yearly=yearly,
            ini_timestamp=from_timestamp,
            filter_zero_revenue=filter_zero_revenue,
        )


class frontend_analytics_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/{chain}/{hypervisor_address}/analytics/positions",
            endpoint=self.positions_status,
            methods=["GET"],
        )

        router.add_api_route(
            path="/{chain}/analytics/correlation",
            endpoint=self.correlation,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def positions_status(
        self,
        response: Response,
        chain: Chain,
        hypervisor_address: str = Query(..., description=" hypervisor address"),
        from_timestamp: int
        | None = Query(
            None,
            description=" limit the data returned from this value. When not set, it will return the last 14 days.",
        ),
        to_timestamp: int
        | None = Query(None, description=" limit the data returned to this value"),
    ) -> list[dict]:
        """Returns data regarding the base and limit positions of a given hypervisor for a given period of time.

        * **symbol**: hypervisor symbol
        * **timestamp**: unix timestamp
        * **block**:  block number
        * **currentTick**: pool tick
        * **baseUpper**: base position upper tick
        * **baseLower**:  base position lower tick
        * **baseLiquidity_usd**:  base position liquidity in USD, using current token prices
        * **limitUpper**: limit position upper tick
        * **limitLower**:  limit position lower tick
        * **limitLiquidity_usd**: limit position liquidity in USD, using current token prices

        """

        if not from_timestamp:
            # set from_timestamp to 14 days ago
            from_timestamp = int(
                datetime.now(tz=timezone.utc).timestamp() - (14 * 24 * 60 * 60)
            )

        # make sure address is valid
        hypervisor_address = filter_addresses([hypervisor_address])[0]
        # return result
        return await get_positions_analysis(
            chain=chain,
            hypervisor_address=hypervisor_address,
            ini_timestamp=from_timestamp,
            end_timestamp=to_timestamp,
        )

    @cache(expire=LONG_CACHE_TIMEOUT)
    async def correlation(
        self,
        response: Response,
        chain: Chain,
        token_addresses: list[str] = Query(None, description=" token addresses"),
        hypervisor_addresses: list[str] = Query(
            None, description=" hypervisor addresses"
        ),
    ):
        """Returns the usd price correlation between tokens, using the last 6000 prices found for the specified tokens.

        ### Query parameters
        * **chain** Chain to filter by.
        * **token_addresses** Token addresses to filter by.
        * **hypervisor_addresses** Hypervisor addresses to filter by.

         ( you must provide either token_addresses or hypervisor_address )

        """

        token_addresses = filter_addresses(token_addresses)
        hypervisor_addresses = filter_addresses(hypervisor_addresses)

        if token_addresses:
            return await get_correlation(
                chains=[chain], token_addresses=token_addresses
            )
        elif hypervisor_addresses:
            return await get_correlation_from_hypervisors(
                chain=chain, hypervisor_addresses=hypervisor_addresses
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You must provide either token_addresses or hypervisor_address",
            )


