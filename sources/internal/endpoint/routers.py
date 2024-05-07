import asyncio
from datetime import datetime, timezone
import logging
import typing
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi.responses import StreamingResponse
from fastapi_cache.decorator import cache
from endpoint.config.cache import DB_CACHE_TIMEOUT, DAILY_CACHE_TIMEOUT

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.formulas.fees import convert_feeProtocol
from sources.common.general.enums import int_to_chain
from sources.common.general.utils import filter_addresses
from sources.internal.bins.internal import (
    InternalFeeReturnsOutput,
    InternalFeeYield,
    InternalGrossFeesOutput,
    InternalTimeframe,
    InternalTokens,
)
from sources.internal.bins.kpis import (
    get_average_tvl,
    get_transactions,
    get_transactions_summary,
    get_users_activity,
)
from sources.internal.bins.reports import (
    custom_report,
    get_kpis_dashboard,
    global_report_revenue,
    report_galaxe,
)
from sources.internal.bins.user import get_user_addresses, get_user_shares

from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import DEPLOYMENTS
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all

from ..bins.fee_internal import (
    get_chain_usd_fees,
    get_gross_fees,
    get_revenue_operations,
)

# Route builders


def build_routers() -> list:
    routes = []

    # MAIN
    routes.append(
        internal_router_builder_main(tags=["Internal endpoints"], prefix="/internal")
    )

    # KPIs
    routes.append(internal_router_builder_KPIs(tags=["KPIs"], prefix="/internal/kpi"))

    # REPORTS
    routes.append(
        internal_router_builder_reports(tags=["Reports"], prefix="/internal/reports")
    )

    return routes


# Route underlying functions


class internal_router_builder_main(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/{protocol}/{chain}/returns",
            endpoint=self.fee_returns,
            methods=["GET"],
        )

        router.add_api_route(
            path="/{chain}/gross_fees",
            endpoint=self.gross_fees,
            methods=["GET"],
        )

        router.add_api_route(
            path="/all_fees",
            endpoint=self.all_chain_usd_fees,
            methods=["GET"],
        )

        router.add_api_route(
            path="/weekly_fees",
            endpoint=self.weekly_chain_usd_fees,
            methods=["GET"],
        )

        router.add_api_route(
            path="/{chain}/revenue",
            endpoint=self.get_revenue,
            methods=["GET"],
        )

        ## MERKL REWARD RELATED
        router.add_api_route(
            path="/user_addresses",
            endpoint=self.user_addresses,
            methods=["GET"],
        )
        router.add_api_route(
            path="/user_shares",
            endpoint=self.user_shares,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    async def fee_returns(
        self, protocol: Protocol, chain: Chain, response: Response
    ) -> dict[str, InternalFeeReturnsOutput]:
        """Returns APR and APY for specific protocol and chain"""
        if (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        results = await asyncio.gather(
            fee_returns_all(protocol, chain, 1, return_total=True),
            fee_returns_all(protocol, chain, 7, return_total=True),
            fee_returns_all(protocol, chain, 30, return_total=True),
            return_exceptions=True,
        )

        result_map = {"daily": results[0], "weekly": results[1], "monthly": results[2]}

        output = {}

        valid_results = (
            (
                result_map["monthly"]["lp"]
                if isinstance(result_map["weekly"], Exception)
                else result_map["weekly"]["lp"]
            )
            if isinstance(result_map["daily"], Exception)
            else result_map["daily"]["lp"]
        )

        for hype_address in valid_results:
            output[hype_address] = InternalFeeReturnsOutput(
                symbol=valid_results[hype_address]["symbol"]
            )

            for period_name, period_result in result_map.items():
                if isinstance(period_result, Exception):
                    continue
                status_total = period_result["total"][hype_address]["status"]
                status_lp = period_result["lp"][hype_address]["status"]
                setattr(
                    output[hype_address],
                    period_name,
                    InternalFeeYield(
                        totalApr=period_result["total"][hype_address]["feeApr"],
                        totalApy=period_result["total"][hype_address]["feeApy"],
                        lpApr=period_result["lp"][hype_address]["feeApr"],
                        lpApy=period_result["lp"][hype_address]["feeApy"],
                        status=f"Total:{status_total}, LP: {status_lp}",
                    ),
                )

        return output

    async def gross_fees(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> dict[str, InternalGrossFeesOutput]:
        """
        Calculates the gross fees aquired (not uncollected) in a period of time for a specific protocol and chain using the protocol fee switch data.

        * When no timeframe is provided, it returns all available data.

        * The **usd** field is calculated using the current (now) price of the token.

        * **protocolFee_X** is the percentage of fees going to the protocol, from 1 to 100.

        * **collected fees** are the fees collected on rebalance and zeroBurn events.
            * *alwaysInPosition* is the definition of a position that has been in range for the whole period.

            * gamma_vs_pool_liquidity_ini: percentage of liquidity gamma has in the pool at the start of the period
            * gamma_vs_pool_liquidity_end: percentage of liquidity gamma has in the pool at the end of the period
            * feeTier: percentage of fee the pool is charging on swaps
            * eVolume: estimated volume in usd ( feeTier/calculated gross fees,  using the current price of the token)
        """
        if protocol and (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        return await get_gross_fees(
            chain=chain,
            protocol=protocol,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    async def all_chain_usd_fees(
        self,
        response: Response,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        protocol: Protocol | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> dict:
        """
        Returns the total current priced USD fees collected (not uncollected) in a period of time for a specific chain
        It uses the "gross fees" point above as underlying data.

        * When no timeframe is provided, it returns all available data.

        * all values are using the current (now) usd price of the token.

        * **collected fees** are the fees collected on rebalance and zeroBurn events.

        * **collectedFees_perDay** are the daily fees collected in the period.
        """

        if isinstance(chain, int):
            chain = int_to_chain(chain)

        if protocol and chain and (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        if not chain:
            output = {}
            requests = [
                get_chain_usd_fees(
                    chain=cha,
                    protocol=None,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    start_block=start_block,
                    end_block=end_block,
                )
                for cha in Chain
            ]

            items = await asyncio.gather(*requests)
            for item in items:
                for k, v in item.items():
                    if not k in output:
                        output[k] = v
                    elif k not in ["weeknum", "timestamp"]:
                        output[k] += v

            return output

        else:
            return await get_chain_usd_fees(
                chain=chain,
                protocol=protocol,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                start_block=start_block,
                end_block=end_block,
            )

    async def weekly_chain_usd_fees(
        self,
        response: Response,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        week_start_timestamp: int | str = "last",
        protocol: Protocol | None = None,
    ) -> list[dict]:
        """
        Returns the total current priced USD fees collected (not uncollected) in a period of time for a specific chain
        It uses the "gross fees" point above as underlying data.
        **week_start_timestamp**: 'last-2' or timestamp can be provided ( last-2 meaning 3 weeks ago)

        * weeks start on monday and end on sunday. All dates used are in UTC. ( timestamp converted )

        * all values are using the current (now) usd price of the token.

        * **collected fees** are the fees collected on rebalance and zeroBurn events.

        * **collectedFees_perDay** are the daily fees collected in the period.

        """
        week_in_seconds = 604800

        if isinstance(chain, int):
            chain = int_to_chain(chain)
        if isinstance(week_start_timestamp, str):
            _now = datetime.now(timezone.utc)
            if week_start_timestamp == "last":
                # calculate last week start timestamp
                week_start_timestamp = (
                    datetime(
                        year=_now.year,
                        month=_now.month,
                        day=_now.day,
                        tzinfo=timezone.utc,
                    ).timestamp()
                    - week_in_seconds
                )
            elif (
                week_start_timestamp.startswith("last")
                and len(week_start_timestamp.split("-")) == 2
            ):
                # calculate last week start timestamp
                week_start_timestamp = datetime(
                    year=_now.year,
                    month=_now.month,
                    day=_now.day,
                    tzinfo=timezone.utc,
                ).timestamp() - (
                    week_in_seconds * (int(week_start_timestamp.split("-")[1]) + 1)
                )
            else:
                raise HTTPException(
                    status_code=400, detail=f"Invalid week start timestamp."
                )

        # get current timestamp
        start_timestamp = week_start_timestamp or int(
            datetime.now(timezone.utc).timestamp()
        )
        end_timestamp = int(datetime.now(timezone.utc).timestamp())

        # weeks in the period
        weeks = int(end_timestamp - start_timestamp) // week_in_seconds

        # create a list of start and end timestamps for each week in the period
        week_timestamps = [
            (
                week,
                start_timestamp + (week_in_seconds * week),
                start_timestamp + (week_in_seconds * (week + 1)) - 1,
            )
            for week in range(weeks)
        ]

        # build requests
        requests = []
        if not chain:
            requests = [
                get_chain_usd_fees(
                    chain=cha,
                    protocol=None,
                    start_timestamp=st,
                    end_timestamp=et,
                    weeknum=weeknum + 1,
                )
                for weeknum, st, et in week_timestamps
                for cha in Chain
            ]
        else:
            # build output structure for each week
            requests = [
                get_chain_usd_fees(
                    chain=chain,
                    protocol=protocol,
                    start_timestamp=st,
                    end_timestamp=et,
                    weeknum=weeknum + 1,
                )
                for weeknum, st, et in week_timestamps
            ]

        # get all data
        result = await asyncio.gather(*requests)

        # build output structure for each week
        if not chain:
            output = {}
            for item_output in result:
                # sum values from the same weeknumk
                if not item_output["weeknum"] in output:
                    output[item_output["weeknum"]] = {}
                for k, v in item_output.items():
                    if not k in output[item_output["weeknum"]]:
                        output[item_output["weeknum"]][k] = v
                    elif k not in ["weeknum", "timestamp"]:
                        output[item_output["weeknum"]][k] += v

            # build output structure for each week. Convert eack key value to item
            output = list(output.values())

        else:
            output = result

        # return output
        return output

    @cache(expire=DB_CACHE_TIMEOUT)
    async def get_revenue(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value.",
        ),
        yearly: bool = Query(
            False,
            description="will group the data by year.",
        ),
    ) -> list[dict]:
        """Returns Gamma's revenue in a period of time for a specific protocol and chain"""
        return await get_revenue_operations(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp,
            end_timestamp=end_timestamp,
            yearly=yearly,
        )

    # USER MERKL REWARD RELATED #################################################################
    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_addresses(
        self,
        response: Response,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        hypervisor_address: str | None = Query(None, description="hypervisor address"),
    ):
        """Returns known user addresses"""
        if isinstance(chain, int):
            chain = int_to_chain(chain)

        if chain:
            return await get_user_addresses(
                chain=chain, hypervisor_address=hypervisor_address
            )
        else:
            return await asyncio.gather(
                *[
                    get_user_addresses(chain=cha, hypervisor_address=hypervisor_address)
                    for cha in list(Chain)
                ]
            )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_shares(
        self,
        response: Response,
        address: str,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        hypervisor_address: str | None = Query(None, description="hypervisor address"),
        timestamp_ini: int | None = Query(
            None, description="will limit the data returned from this value."
        ),
        timestamp_end: int | None = Query(
            None, description="will limit the data returned to this value."
        ),
        block_ini: int | None = Query(
            None, description="will limit the data returned from this value."
        ),
        block_end: int | None = Query(
            None, description="will limit the data returned to this value."
        ),
        include_operations: bool = Query(
            False,
            description="will include a justifying operations list",
        ),
    ):
        """Returns shares for a given user address"""

        if isinstance(chain, int):
            chain = int_to_chain(chain)
        address = filter_addresses(address)
        hypervisor_address = filter_addresses(hypervisor_address)

        if chain:
            return await get_user_shares(
                user_address=address,
                chain=chain,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
                block_ini=block_ini,
                block_end=block_end,
                hypervisor_address=hypervisor_address,
                include_operations=include_operations,
            )
        else:
            return await asyncio.gather(
                *[
                    get_user_shares(
                        user_address=address,
                        chain=cha,
                        timestamp_ini=timestamp_ini,
                        timestamp_end=timestamp_end,
                        block_ini=block_ini,
                        block_end=block_end,
                        hypervisor_address=hypervisor_address,
                        include_operations=include_operations,
                    )
                    for cha in list(Chain)
                ]
            )


class internal_router_builder_KPIs(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/{chain}/average_tvl",
            endpoint=self.average_tvl,
            methods=["GET"],
        )
        #
        router.add_api_route(
            path="/{chain}/transactions",
            endpoint=self.transactions,
            methods=["GET"],
        )

        router.add_api_route(
            path="/{chain}/transactions_summary",
            endpoint=self.transactions_summary,
            methods=["GET"],
        )

        router.add_api_route(
            path="/user_activity",
            endpoint=self.users_activity,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def average_tvl(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value. When no value is provided, last available timestamp data will be used.",
        ),
        hypervisors: typing.List[str] = Query(
            None, description="List of hypervisor addresses to filter"
        ),
    ) -> dict:
        """Returns the average TVL for a given period of time, using the current price of the token.
        * **average TVL** = (sum of TVL) / (number of TVL in the period)

        **IMPORTANT CONSIDERATION**: It uses database snapshots so the inactive hypervisors ( without any event [withraw, rebalance, deposit, zeroBurn...] within the period) will not be included in the average TVL calculation.

        """

        # # do not allow ini_timestamp to be greater than 120 days ago
        # if ini_timestamp and (
        #     datetime.now(timezone.utc).timestamp() - ini_timestamp > 86400 * 120
        # ):
        #     raise HTTPException(
        #         status_code=400,
        #         detail=f"ini_timestamp cannot be greater than 120 days ago.",
        #     )

        # set ini timestamp to 7 days ago if not provided
        return await get_average_tvl(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=filter_addresses(hypervisors),
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def transactions(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value. When no value is provided, last available timestamp data will be used.",
        ),
        hypervisors: typing.List[str] = Query(
            None, description="List of hypervisor addresses to filter"
        ),
    ) -> dict:
        """Returns transactions summary:
        usd value and quantities of **withdraws** and **deposits** and quantities of **shares** deposited/withdrawn, **transfers**, **approvals**, **zeroBurns** and **rebalances**


        All usd values are calculated using the current price of the token.
        """

        # do not allow ini_timestamp to be greater than 240 days ago
        # if ini_timestamp and (
        #     datetime.now(timezone.utc).timestamp() - ini_timestamp > 86400 * 240
        # ):
        #     raise HTTPException(
        #         status_code=400,
        #         detail=f"ini_timestamp cannot be greater than 240 days ago.",
        #     )

        # set ini timestamp to 7 days ago if not provided
        return await get_transactions(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=filter_addresses(hypervisors),
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def transactions_summary(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value. When no value is provided, last available timestamp data will be used.",
        ),
        hypervisors: typing.List[str] = Query(
            None, description="List of hypervisor addresses to filter"
        ),
    ):
        """Returns a transactions summary:
        * **tvl_variation_usd** is deposits - withdraws + collected and uncollected fees
        * **new_users_usd** is deposits - withdraws
        * **fees_usd** is collected + uncollected fees
        * **gross_fees_usd** is fees_usd / (1 - protocolFee)
        * **volume** is gross_fees_usd / feeTier
        * **details** All the data used to calculate the summary


        All usd values are calculated using the current price of the token.
        """

        return await get_transactions_summary(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=filter_addresses(hypervisors),
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def users_activity(
        self,
        response: Response,
        chain: Chain | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value. When no value is provided, last available timestamp data will be used.",
        ),
        hypervisors: typing.List[str] = Query(
            None, description="List of hypervisor addresses to filter"
        ),
    ):
        """Returns a list of unique users activity, measured as deposits and withdraws."""
        return await get_users_activity(
            chain=chain,
            ini_timestamp=ini_timestamp,
            end_timestamp=end_timestamp,
            hypervisors=filter_addresses(hypervisors),
        )


class internal_router_builder_reports(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        router = APIRouter(prefix=self.prefix)

        #
        router.add_api_route(
            path="/galxe_report",
            endpoint=self.galxe_report,
            methods=["GET"],
        )
        #
        router.add_api_route(
            path="/global_revenue",
            endpoint=self.global_revenue,
            methods=["GET"],
        )

        router.add_api_route(
            path="/custom",
            endpoint=self.custom_report,
            methods=["GET"],
        )

        router.add_api_route(
            path="/kpi_dashboard",
            endpoint=self.kpi_dashboard,
            methods=["GET"],
        )

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def galxe_report(
        self,
        response: Response,
        chain: Chain | int = Query(
            Chain.MANTLE,
            enum=[
                Chain.MANTLE,
                # Chain.ARBITRUM,
                Chain.POLYGON,
                Chain.MANTLE.id,
                # Chain.ARBITRUM.id,
                Chain.POLYGON.id,
            ],
        ),
        user_address: str | None = Query(
            None, description="User address to filter the report"
        ),
        net_position_usd_threshold: int | None = Query(
            None,
            description="Minimum USD net value during the whole period (deposits-withdraws).",
        ),
        deposits_usd_threshold: int | None = Query(
            100, description="Minimum USD deposits value during the whole period."
        ),
    ) -> dict:
        """Returns unique list of user addresses complying with the parameters defined (net position | deposits) and within a list of predefined pools between start and end time:
        * **Pools**:
            * MANTLE:
                * **Start time**:  May 8th, 16:00 UTC
                * **End time**:  June 7th, 16:00 UTC
                * 0x6e9d701fb6478ed5972a37886c2ba6c82a4cbb4c
                0xd6cc4a33da7557a629e819c68fb805ddb225f517
                0xde7421f870ffb2b99998d9ed07c4d9b22e783922
                0x561f5cf838429586d1f8d3826526891b289270ee
                0xfa81e2922b084ab260f7f8abd1d455d1235688d0
                0xc0766ff871c6c8e72c110100d0120829dc017d38
                0x78727b60f684f753152990dfa67e5c3940692279
                0x099dd23eaab20f5ec43f50055d6e3030c66cc182
                0x89bd0737f2b860535711678259b7fb931f493344
            * POLYGON:
                * **Start time**:  May 8th, 16:00 UTC
                * **End time**:  June 7th, 16:00 UTC
                * 0x1Fd452156b12FB5D74680C5Ff166303E6dd12A78
                0xE583b04b9a8F576aa7F17ECc6eB662499B5A8793
                0x831231e16d95eb3d54bf2c80968f35a5f4483447
                0x8c6fe430cf06e56be7c092ad3a249bf0bcb388b9

        All usd values are calculated using prices at operation block ( at the time the operation happened).
        """

        return await report_galaxe(
            chain=chain,
            user_address=filter_addresses(user_address),
            net_position_usd_threshold=net_position_usd_threshold,
            deposits_usd_threshold=deposits_usd_threshold,
        )

    # @cache(expire=DAILY_CACHE_TIMEOUT)
    async def global_revenue(self, response: Response) -> dict:
        """Global revenue report
        * **total_usd**:  Total revenue in USD
        * **potential total usd**:  Current month's total_usd 50% decay linear extrapolation [ total_usd/days_passed * 0.5 * days left + total_usd ].
        * **yearly_percentage**:  this total_usd / year's total_usd
        * **monthly_percentage**:  this total_usd / month's total_usd

        """
        return await global_report_revenue()

    async def custom_report(self, response: Response, user_address: str | None = None):
        """Custom report"""

        reports = await custom_report(
            chain=Chain.ARBITRUM, user_address=filter_addresses(user_address)
        )

        return reports

    async def kpi_dashboard(
        self,
        response: Response,
        chain: Chain | int | None = Query(None, enum=[*Chain, *[x.id for x in Chain]]),
        protocol: Protocol | None = None,
        ini_timestamp: int | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int | None = Query(
            None,
            description="will limit the data returned to this value. When no value is provided, last available timestamp data will be used.",
        ),
        period_seconds: int | None = Query(
            None,
            description="will group the data in periods of this length in seconds.",
        ),
        hypervisor_addresses: typing.List[str] | None = Query(
            None, description="List of hypervisor addresses to filter"
        ),
    ):

        if isinstance(chain, int):
            chain = int_to_chain(chain)

        if protocol and chain and (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        # build filename
        _filename = f"{chain.id}"
        if protocol:
            _filename += f"_{protocol}"
        if hypervisor_addresses:
            for hypervisor_address in hypervisor_addresses:
                _filename += f"_{hypervisor_address}"
        if ini_timestamp:
            _filename += f"_{ini_timestamp}"
        if end_timestamp:
            _filename += f"_{end_timestamp}"
        if period_seconds:
            _filename += f"_{period_seconds}"

        _filename += "_kpiDashboard.csv"

        return StreamingResponse(
            content=iter(
                [
                    await get_kpis_dashboard(
                        chain=chain,
                        protocol=protocol,
                        ini_timestamp=ini_timestamp,
                        end_timestamp=end_timestamp,
                        period_seconds=period_seconds,
                        hypervisor_addresses=filter_addresses(hypervisor_addresses),
                    )
                ]
            ),
            media_type="text/csv",
            headers={f"Content-Disposition": f"attachment; filename={_filename}"},
        )
