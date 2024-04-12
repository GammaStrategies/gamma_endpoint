import asyncio
from datetime import datetime, timezone
import logging
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi.responses import StreamingResponse
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
from sources.common.general.enums import Period, int_to_chain, int_to_period
from sources.common.general.utils import filter_addresses
from sources.frontend.bins.analytics import (
    build_hypervisor_returns_graph,
    get_positions_analysis,
)
from sources.frontend.bins.correlation import (
    get_correlation_from_hypervisors,
)
from sources.frontend.bins.external_apis import get_ramsesLike_api_data
from sources.frontend.bins.revenue_stats import get_revenue_stats
from sources.frontend.bins.users import get_user_positions

from sources.mongo.bins.apps.returns import build_hype_return_analysis_from_database
from sources.subgraph.bins.enums import Chain, Protocol


# Route builders


def build_routers() -> list:
    routes = []

    routes.append(
        frontend_revenueStatus_router_builder_main(tags=["Revenue status"], prefix="")
    )
    routes.append(frontend_analytics_router_builder_main(tags=["Analytics"], prefix=""))

    routes.append(frontend_user_router_builder_main(tags=["User"], prefix=""))

    routes.append(
        frontend_externalApis_router_builder_main(tags=["External APIs"], prefix="")
    )

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
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        protocol: Protocol | None = None,
        from_timestamp: int | None = None,
        yearly: bool = False,
        monthly: bool = False,
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
        * **monthly** group result by month.
        * **filter_zero_revenue** filter out zero revenue items.

        """

        if isinstance(chain, int):
            chain = int_to_chain(chain)

        return await get_revenue_stats(
            chain=chain,
            protocol=protocol,
            yearly=yearly,
            monthly=monthly,
            ini_timestamp=from_timestamp,
            filter_zero_revenue=filter_zero_revenue,
        )


class frontend_analytics_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        # # TODO: DELETE. This is replaced by its next route to the function
        # router.add_api_route(
        #     path="/{chain}/{hypervisor_address}/analytics/returns/chart",
        #     endpoint=self.hypervisor_analytics_return_graph,
        #     methods=["GET"],
        # )

        router.add_api_route(
            path="/analytics/returns/chart",
            endpoint=self.hypervisor_analytics_return_graph,
            methods=["GET"],
        )

        router.add_api_route(
            path="/analytics/returns/csv",
            endpoint=self.hypervisor_analytics_return_detail,
            methods=["GET"],
        )

        router.add_api_route(
            path="/analytics/positions",
            endpoint=self.positions_status,
            methods=["GET"],
        )

        router.add_api_route(
            path="/analytics/correlation",
            endpoint=self.correlation,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def positions_status(
        self,
        response: Response,
        hypervisor_address: str,
        chain: Chain | int = Query(
            Chain.ARBITRUM, enum=[*Chain, *[x.id for x in Chain]]
        ),
        period: Period | int = Query(
            Period.BIWEEKLY, enum=[*Period, *[x.days for x in Period]]
        ),
        from_timestamp: int | None = Query(
            None,
            description=" limit the data returned from this value. When not set, it will return the last 14 days.",
        ),
        to_timestamp: int | None = Query(
            None, description=" limit the data returned to this value"
        ),
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
        # convert to chain and period
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        if isinstance(period, int):
            period = int_to_period(period)
        hypervisor_address = filter_addresses(hypervisor_address)

        if not from_timestamp and period:
            # Period exists and from_timestamp is not set
            # convert period to timestamp: current timestamp in utc timezone
            from_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - (
                (period.days * 24 * 60 * 60)
                if period != Period.DAILY
                else period.days * 24 * 2 * 60 * 60
            )

        elif not period:
            # set from_timestamp to 14 days ago
            from_timestamp = int(
                datetime.now(tz=timezone.utc).timestamp() - (14 * 24 * 60 * 60)
            )
        elif from_timestamp and period:
            # both from_timestamp and period are set
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="You must provide either period or from_timestamp",
            )

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
        chain: Chain | int = Query(
            Chain.ARBITRUM, enum=[*Chain, *[x.id for x in Chain]]
        ),
        period: Period | int = Query(
            Period.BIWEEKLY, enum=[*Period, *[x.days for x in Period]]
        ),
        hypervisor_address: str = Query(..., description=" hypervisor addresses"),
    ):
        """Returns the usd price correlation between tokens.
        (  1 = correlated    -1 = inversely correlated )

        """

        if isinstance(chain, int):
            chain = int_to_chain(chain)
        if isinstance(period, int):
            period = int_to_period(period)
        hypervisor_address = filter_addresses(hypervisor_address)

        # convert period to timestamp: current timestamp in utc timezone
        from_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - (
            (period.days * 24 * 60 * 60)
            if period != Period.DAILY
            else period.days * 24 * 2 * 60 * 60
        )

        return await get_correlation_from_hypervisors(
            chain=chain,
            hypervisor_addresses=[hypervisor_address],
            from_timestamp=from_timestamp,
        )

    @cache(expire=DAILY_CACHE_TIMEOUT)
    async def hypervisor_analytics_return_graph(
        self,
        response: Response,
        hypervisor_address: str,
        chain: Chain | int = Query(
            Chain.ARBITRUM, enum=[*Chain, *[x.id for x in Chain]]
        ),
        period: Period | int = Query(
            Period.BIWEEKLY, enum=[*Period, *[x.days for x in Period]]
        ),
    ):
        """Hypervisor returns data within the period, including token0 and token1 prices:

        * **timestamp**: unix timestamp

        """
        # convert
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        if isinstance(period, int):
            period = int_to_period(period)
        hypervisor_address = filter_addresses(hypervisor_address)

        # convert period to timestamp: current timestamp in utc timezone
        ini_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - (
            (period.days * 24 * 60 * 60)
            if period != Period.DAILY
            else period.days * 24 * 2 * 60 * 60
        )

        return await build_hypervisor_returns_graph(
            chain=chain,
            hypervisor_address=hypervisor_address,
            ini_timestamp=ini_timestamp,
            points_every=(60 * 60) if period == Period.DAILY else (60 * 60 * 12),
        )

    # Hypervisor returns ( no cache for csv files )
    async def hypervisor_analytics_return_detail(
        self,
        response: Response,
        hypervisor_address: str,
        chain: Chain | int = Query(
            Chain.ARBITRUM, enum=[*Chain, *[x.id for x in Chain]]
        ),
        period: Period | int = Query(
            Period.BIWEEKLY, enum=[*Period, *[x.days for x in Period]]
        ),
    ):
        """Return a csv file containing all hypervisor returns details with respect to the specified period returns"""
        # convert
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        if isinstance(period, int):
            period = int_to_period(period)
        hypervisor_address = filter_addresses(hypervisor_address)

        # convert period to timestamp: current timestamp in utc timezone
        ini_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - (
            (period.days * 24 * 60 * 60)
            if period != Period.DAILY
            else period.days * 24 * 2 * 60 * 60
        )

        # try get data
        hype_return_analysis = await build_hype_return_analysis_from_database(
            chain=chain,
            hypervisor_address=hypervisor_address,
            ini_timestamp=ini_timestamp,
        )
        if not hype_return_analysis:
            # try get data using the latest collection
            hype_return_analysis = await build_hype_return_analysis_from_database(
                chain=chain,
                hypervisor_address=hypervisor_address,
                ini_timestamp=ini_timestamp,
                use_latest_collection=True,
            )
        if not hype_return_analysis:
            response.status_code = status.HTTP_404_NOT_FOUND
            return {"detail": "No data found for the given parameters"}

        _filename = (
            f"{chain.fantasy_name}_{hypervisor_address}_{period.name}_returns.csv"
        )
        return StreamingResponse(
            content=iter([hype_return_analysis.get_graph_csv()]),
            media_type="text/csv",
            headers={f"Content-Disposition": f"attachment; filename={_filename}"},
        )


class frontend_user_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        router.add_api_route(
            path="/user/positions",
            endpoint=self.user_positions,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_positions(
        self,
        response: Response,
        address: str,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
    ):
        """Returns all positions for a given user address"""
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        address = filter_addresses(address)

        if chain:
            return await get_user_positions(user_address=address, chain=chain)
        else:
            return await asyncio.gather(
                *[
                    get_user_positions(user_address=address, chain=cha)
                    for cha in list(Chain)
                ]
            )


class frontend_externalApis_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        router.add_api_route(
            path="/externalApis/ramsesLike",
            endpoint=self.get_rewardsApr,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def get_rewardsApr(
        self,
        response: Response,
        chain: Chain | int = Query(
            Chain.ARBITRUM, enum=[*Chain, *[x.id for x in Chain]]
        ),
        protocol: Protocol = Query(
            Protocol.RAMSES,
            enum=[Protocol.RAMSES, Protocol.CLEOPATRA, Protocol.PHARAOH],
        ),
    ):
        """Get rewards APR for a given chain and protocol."""

        # check if protocol is in the list
        if protocol not in [
            Protocol.RAMSES,
            Protocol.CLEOPATRA,
            Protocol.PHARAOH,
        ]:
            raise HTTPException(
                status_code=400,
                detail="Protocol not supported. Supported protocols are: ramses, cleopatra, pharaoh",
            )

        try:
            if isinstance(chain, int):
                chain = int_to_chain(chain)

            return await get_ramsesLike_api_data(chain=chain, protocol=protocol)
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error getting rewards APR")
