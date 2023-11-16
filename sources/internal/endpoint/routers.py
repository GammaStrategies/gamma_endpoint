import asyncio
from datetime import datetime, timezone
import logging
import typing
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi_cache.decorator import cache
from endpoint.config.cache import DB_CACHE_TIMEOUT, DAILY_CACHE_TIMEOUT

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.formulas.fees import convert_feeProtocol
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
)
from sources.mongo.bins.apps.hypervisor import hypervisors_collected_fees
from sources.mongo.bins.apps.prices import get_current_prices
from sources.mongo.bins.helpers import local_database_helper

from sources.common.database.collection_endpoint import database_global, database_local
from sources.common.database.common.collections_common import db_collections_common

from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import DEPLOYMENTS
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all

from ..bins.fee_internal import (
    get_chain_usd_fees,
    get_fees,
    get_gross_fees,
    get_revenue_operations,
)

# Route builders


def build_routers() -> list:
    routes = []

    routes.append(
        internal_router_builder_main(tags=["Internal endpoints"], prefix="/internal")
    )

    routes.append(internal_router_builder_KPIs(tags=["KPIs"], prefix="/internal/kpi"))

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
        chain: Chain,
        response: Response,
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
        chain: Chain | None = None,
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
        if protocol and (protocol, chain) not in DEPLOYMENTS:
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
        chain: Chain | None = None,
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
        ini_timestamp: int
        | None = Query(
            None,
            description="will limit the data returned from this value",
        ),
        end_timestamp: int
        | None = Query(
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

        return router

    # ROUTE FUNCTIONS
    @cache(expire=DB_CACHE_TIMEOUT)
    async def average_tvl(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int
        | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int
        | None = Query(
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

        # do not allow ini_timestamp to be greater than 120 days ago
        if ini_timestamp and (
            datetime.now(timezone.utc).timestamp() - ini_timestamp > 86400 * 120
        ):
            raise HTTPException(
                status_code=400,
                detail=f"ini_timestamp cannot be greater than 120 days ago.",
            )

        hypervisors = filter_addresses(hypervisors)

        # set ini timestamp to 7 days ago if not provided
        return await get_average_tvl(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=hypervisors,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def transactions(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int
        | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int
        | None = Query(
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

        # do not allow ini_timestamp to be greater than 120 days ago
        if ini_timestamp and (
            datetime.now(timezone.utc).timestamp() - ini_timestamp > 86400 * 120
        ):
            raise HTTPException(
                status_code=400,
                detail=f"ini_timestamp cannot be greater than 120 days ago.",
            )

        # remove any empty or 'string' hypervisors
        hypervisors = filter_addresses(hypervisors)

        # set ini timestamp to 7 days ago if not provided
        return await get_transactions(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=hypervisors,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def transactions_summary(
        self,
        response: Response,
        chain: Chain,
        protocol: Protocol | None = None,
        ini_timestamp: int
        | None = Query(
            None,
            description="will limit the data returned from this value. When no value is provided, -7 days will be used",
        ),
        end_timestamp: int
        | None = Query(
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
        # remove any empty or 'string' hypervisors
        hypervisors = filter_addresses(hypervisors)

        return await get_transactions_summary(
            chain=chain,
            protocol=protocol,
            ini_timestamp=ini_timestamp
            or int(datetime.now(timezone.utc).timestamp() - (86400 * 7)),
            end_timestamp=end_timestamp,
            hypervisors=hypervisors,
        )
