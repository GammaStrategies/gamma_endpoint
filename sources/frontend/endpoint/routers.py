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
    explain_hypervisor_returns,
    get_positions_analysis,
)
from sources.frontend.bins.correlation import (
    get_correlation_from_hypervisors,
)
from sources.frontend.bins.external_apis import (
    get_leaderboard_xlayer,
    get_ramsesLike_api_data,
    get_leaderboard,
)
from sources.frontend.bins.revenue_stats import get_revenue_stats
from sources.frontend.bins.users import get_user_positions

from sources.mongo.bins.apps.returns import build_hype_return_analysis_from_database
from sources.subgraph.bins.common.hypervisor import unified_hypervisors_data
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
        frontend_hypervisor_router_builder_main(tags=["Hypervisors"], prefix="")
    )

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
            path="/analytics/returns/xplain",
            endpoint=self.hypervisor_analytics_return_xplanation,
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
        # points_every: int = Query(None, include_in_schema=False),
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

        # if not points_every:
        # set points every hour for daily and every 12 hours for the rest
        points_every = (60 * 60) if period == Period.DAILY else (60 * 60 * 12)

        return await build_hypervisor_returns_graph(
            chain=chain,
            hypervisor_address=hypervisor_address,
            ini_timestamp=ini_timestamp,
            points_every=points_every,
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

        # try get data ( from either the main or the latest collection )
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

    @cache(expire=DAILY_CACHE_TIMEOUT)
    async def hypervisor_analytics_return_xplanation(
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
        """Explain the hypervisor return details with respect to the specified period"""
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

        try:

            # try get data ( from either the main or the latest collection )
            hype_return_analysis = await build_hype_return_analysis_from_database(
                chain=chain,
                hypervisor_address=hypervisor_address,
                ini_timestamp=ini_timestamp,
                use_latest_collection=True,
            )
            if not hype_return_analysis:
                response.status_code = status.HTTP_404_NOT_FOUND
                return {"detail": "No data found for the given parameters"}

            #
            return await explain_hypervisor_returns(
                hype_return_analysis._graph_data[0],
                hype_return_analysis._graph_data[-1],
            )
        except Exception as e:
            logging.error(e)
            raise HTTPException(
                status_code=500, detail="Error getting hypervisor return explanation"
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
        """Returns all positions known for a given user address at the latest block available.
        * Balance **USD amount** is calculated using the last known token prices.
        * Balance **shares_percent** is the percentage of share the user has versus the total shares of the hypervisor.
        * Balance **token0 and token1** are calculated using the shares_percent and the last known total liquidity of the hypervisor.
        * Deposited amounts will not be accounted for when being LP token transfers.
        * Deposited **USD amount** is calculated using the closest prices to the timestamp of the deposit.
        """
        if isinstance(chain, int):
            chain = int_to_chain(chain)
        address = filter_addresses(address)

        if chain:
            return await get_user_positions(user_address=address, chain=chain)
        else:
            items = await asyncio.gather(
                *[
                    get_user_positions(user_address=address, chain=cha)
                    for cha in list(Chain)
                ]
            )
            return [x for xs in items for x in xs]


class frontend_hypervisor_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        router.add_api_route(
            path="/hypervisors/allDataSummary",
            endpoint=self.allDataSummary,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def allDataSummary(self, response: Response):
        """Returns a summary of the status of all hypervisors across all chains.
        This involves querying the allData endpoints for each protocol and chain to retrieve the hypervisors' feeAPR,
        and querying the allRewards2 endpoints for each protocol and chain to obtain the hypervisors' rewards.
        Additionally, it includes rewards from the following sources:
        - Angle Merkl (excluding rewards from different campaign chains like uni)
        - Camelot
        - Hercules
        - Lynex
        - Ramses
        - Synthswap
        - Zyberswap
        - Beamswap
        - Gamma's MultiRewards
        - Gamma's StakingRewards
        """
        # database call only
        return await unified_hypervisors_data()


class frontend_externalApis_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        router.add_api_route(
            path="/externalApis/ramsesLike",
            endpoint=self.get_rewardsApr,
            methods=["GET"],
        )

        router.add_api_route(
            path="/externalApis/leaderboard",
            endpoint=self.get_leaderboard_from_balances,
            methods=["GET"],
        )

        router.add_api_route(
            path="/externalApis/leaderboard_rewards",
            endpoint=self.get_leaderboard_from_rewards,
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

    @cache(expire=DB_CACHE_TIMEOUT)
    async def get_leaderboard_from_balances(
        self,
        response: Response,
        chain: Chain | int = Query(
            Chain.XLAYER,
            enum=[Chain.XLAYER, Chain.XLAYER.id],
            description="Chain to filter by",
        ),
        contracts: bool = Query(
            True,
            description="Include addresses defined as contracts in the result",
        ),
        transfers: bool = Query(
            False,
            description="Include transfers that make up the balance of each user in the result",
        ),
        token_address: str = Query(
            "0xb3fe9cf380e889edf9ada9443d76f1cee328fd07",
            description="Token address to use for building up the leaderboard",
        ),
        exclude_allowedFrom_addresses: bool = Query(
            True,
            description="Exclude addresses defined in setallowedfrom events in the result",
        ),
    ):
        """xLayer token leaderBoard using the balances of wallet addresses ( claimed only)"""

        try:
            return await get_leaderboard(
                chain=chain,
                include_contracts=contracts,
                include_transfers=transfers,
                token_address=token_address,
                exclude_defined_addresses=exclude_allowedFrom_addresses,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail="Error getting leaderboard")

    @cache(expire=DB_CACHE_TIMEOUT)
    async def get_leaderboard_from_rewards(
        self,
        response: Response,
        timestamp: int = Query(
            None,
            description="Timestamp to filter by. If not set, it will return the latest leaderboard. If set, it will return the leaderboard snapshot closest to the timestamp ( lower than or equal)",
        ),
    ):
        """xTrade token leaderBoard using claimed and claimable xPoints"""

        try:
            return await get_leaderboard_xlayer(timestamp=timestamp)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail="Error getting xlayer leaderboard"
            )
