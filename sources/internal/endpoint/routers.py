import asyncio
import logging
from fastapi import HTTPException, Query, Response, APIRouter, status
from fastapi_cache.decorator import cache

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.formulas.fees import convert_feeProtocol
from sources.internal.bins.internal import (
    InternalFeeReturnsOutput,
    InternalFeeYield,
    InternalGrossFeesOutput,
    InternalTimeframe,
    InternalTokens,
)
from sources.mongo.bins.apps.hypervisor import hypervisors_collected_fees
from sources.mongo.bins.apps.prices import get_current_prices
from sources.mongo.bins.helpers import local_database_helper

from sources.common.database.collection_endpoint import database_global, database_local
from sources.common.database.common.collections_common import db_collections_common

from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import DEPLOYMENTS
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all

from ..bins.fee_internal import get_fees

# Route builders


def build_routers() -> list:
    routes = []

    routes.append(
        internal_router_builder_main(tags=["Internal endpoints"], prefix="/internal")
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
            path="/{chain}/all_fees",
            endpoint=self.all_chain_usd_fees,
            methods=["GET"],
        )

        router.add_api_route(
            path="/ramses_gross_fees",
            endpoint=self.get_gross_fees_ramses,
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

    # async def gross_fees(
    #     self,
    #     chain: Chain,
    #     response: Response,
    #     protocol: Protocol | None = None,
    #     start_timestamp: int | None = None,
    #     end_timestamp: int | None = None,
    #     start_block: int | None = None,
    #     end_block: int | None = None,
    # ):
    #     return await get_fees(
    #         chain=chain,
    #         protocol=protocol,
    #         start_timestamp=start_timestamp,
    #         end_timestamp=end_timestamp,
    #         start_block=start_block,
    #         end_block=end_block,
    #     )

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

        """

        if protocol and (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        # create result dict
        output = {}

        # get hypervisors current prices
        token_prices = {
            x["address"]: x for x in await get_current_prices(network=chain)
        }
        # get all hypervisors last status from the database
        query_hype_status = [
            {"$sort": {"block": -1}},
            {
                "$group": {
                    "_id": "$address",
                    "data": {"$first": "$$ROOT"},
                }
            },
        ]
        # filter by protocol
        if protocol:
            query_hype_status.insert(0, {"$match": {"dex": protocol.database_name}})

        hypervisor_status = {
            x["data"]["address"]: x["data"]
            for x in await local_database_helper(network=chain).get_items_from_database(
                collection_name="status",
                aggregate=query_hype_status,
            )
        }

        # get a sumarized data portion for all hypervisors in the database for a period
        # when no period is specified, it will return all available data
        for hype_summary in await local_database_helper(
            network=chain
        ).query_items_from_database(
            collection_name="operations",
            query=database_local.query_operations_summary(
                hypervisor_addresses=list(hypervisor_status.keys()),
                timestamp_ini=start_timestamp,
                timestamp_end=end_timestamp,
                block_ini=start_block,
                block_end=end_block,
            ),
        ):
            # convert to float
            hype_summary = db_collections_common.convert_decimal_to_float(
                item=db_collections_common.convert_d128_to_decimal(item=hype_summary)
            )

            # ease hypervisor static data access
            hype_status = hypervisor_status.get(hype_summary["address"], {})
            if not hype_status:
                logging.getLogger(__name__).error(
                    f"Static data not found for hypervisor {hype_summary['address']}"
                )
                continue
            # ease hypervisor price access
            token0_price = token_prices.get(
                hype_status["pool"]["token0"]["address"], {}
            ).get("price", 0)
            token1_price = token_prices.get(
                hype_status["pool"]["token1"]["address"], {}
            ).get("price", 0)
            if not token0_price or not token1_price:
                logging.getLogger(__name__).error(
                    f"Price not found for token0[{token0_price}] or token1[{token1_price}] of hypervisor {hype_summary['address']}"
                )
                continue

            # calculate protocol fees
            if "globalState" in hype_status["pool"]:
                protocol_fee_0_raw = hype_status["pool"]["globalState"][
                    "communityFeeToken0"
                ]
                protocol_fee_1_raw = hype_status["pool"]["globalState"][
                    "communityFeeToken1"
                ]
            else:
                # convert from 8 decimals
                protocol_fee_0_raw = hype_status["pool"]["slot0"]["feeProtocol"] % 16
                protocol_fee_1_raw = hype_status["pool"]["slot0"]["feeProtocol"] >> 4

            # convert to percent (0-100)
            protocol_fee_0, protocol_fee_1 = convert_feeProtocol(
                feeProtocol0=protocol_fee_0_raw,
                feeProtocol1=protocol_fee_1_raw,
                hypervisor_protocol=hype_status["dex"],
                pool_protocol=hype_status["pool"]["dex"],
            )

            # calculate collected fees
            collectedFees_0 = (
                hype_summary["collectedFees_token0"]
                + hype_summary["zeroBurnFees_token0"]
            )
            collectedFees_1 = (
                hype_summary["collectedFees_token1"]
                + hype_summary["zeroBurnFees_token1"]
            )
            collectedFees_usd = (
                collectedFees_0 * token0_price + collectedFees_1 * token1_price
            )

            if protocol_fee_0 > 100 or protocol_fee_1 > 100:
                logging.getLogger(__name__).warning(
                    f"Protocol fee is >100% for hypervisor {hype_summary['address']}"
                )

            # calculate gross fees
            if protocol_fee_0 < 100:
                grossFees_0 = collectedFees_0 / (1 - (protocol_fee_0 / 100))
            else:
                grossFees_0 = collectedFees_0

            if protocol_fee_1 < 100:
                grossFees_1 = collectedFees_1 / (1 - (protocol_fee_1 / 100))
            else:
                grossFees_1 = collectedFees_1

            grossFees_usd = grossFees_0 * token0_price + grossFees_1 * token1_price

            # days period
            days_period = (
                hype_summary["timestamp_end"] - hype_summary["timestamp_ini"]
            ) / 86400

            # build output
            output[hype_summary["address"]] = InternalGrossFeesOutput(
                symbol=hype_status["symbol"],
                days_period=days_period,
                block=InternalTimeframe(
                    ini=hype_summary["block_ini"],
                    end=hype_summary["block_end"],
                ),
                timestamp=InternalTimeframe(
                    ini=hype_summary["timestamp_ini"],
                    end=hype_summary["timestamp_end"],
                ),
                deposits=InternalTokens(
                    token0=hype_summary["deposits_token0"],
                    token1=hype_summary["deposits_token1"],
                    usd=hype_summary["deposits_token0"] * token0_price
                    + hype_summary["deposits_token1"] * token1_price,
                ),
                withdraws=InternalTokens(
                    token0=hype_summary["withdraws_token0"],
                    token1=hype_summary["withdraws_token1"],
                    usd=hype_summary["withdraws_token0"] * token0_price
                    + hype_summary["withdraws_token1"] * token1_price,
                ),
                collectedFees=InternalTokens(
                    token0=collectedFees_0,
                    token1=collectedFees_1,
                    usd=collectedFees_usd,
                ),
                protocolFee_0=protocol_fee_0,
                protocolFee_1=protocol_fee_1,
                calculatedGrossFees=InternalTokens(
                    token0=grossFees_0,
                    token1=grossFees_1,
                    usd=grossFees_usd,
                ),
            )

        return output

    async def all_chain_usd_fees(
        self,
        chain: Chain,
        response: Response,
        protocol: Protocol | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> dict:
        """
        Returns the total current priced USD fees collected in a period of time for a specific chain

        * When no timeframe is provided, it returns all available data.

        * The **usd** field is calculated using the current (now) price of the token.

        * **collectedFees_perDay** are the daily fees collected in the period.
        """
        if protocol and (protocol, chain) not in DEPLOYMENTS:
            raise HTTPException(
                status_code=400, detail=f"{protocol} on {chain} not available."
            )

        output = {
            "hypervisors": 0,
            "deposits": 0,
            "withdraws": 0,
            "collectedFees": 0,
            "calculatedGrossFees": 0,
            "collectedFees_perDay": 0,
        }
        data = await self.gross_fees(
            chain=chain,
            response=response,
            protocol=protocol,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )
        for hypervisor_address, hypervisor_data in data.items():
            output["hypervisors"] += 1
            output["deposits"] += hypervisor_data.deposits.usd
            output["withdraws"] += hypervisor_data.withdraws.usd
            output["collectedFees"] += hypervisor_data.collectedFees.usd
            output["calculatedGrossFees"] += hypervisor_data.calculatedGrossFees.usd
            output["collectedFees_perDay"] += (
                (hypervisor_data.collectedFees.usd / hypervisor_data.days_period)
                if hypervisor_data.days_period
                else 0
            )

        return output

    async def get_report(
        self,
        chain: Chain,
        response: Response,
        protocol: Protocol | None = None,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        find = {"type": "gross_fees"}
        if protocol:
            find["protocol"] = protocol.database_name
        if start_timestamp:
            find["timeframe.ini.timestamp"] = {"$gte": start_timestamp}
        if end_timestamp:
            find["timeframe.end.timestamp"] = {"$lte": end_timestamp}
        if start_block:
            find["timeframe.ini.block"] = {"$gte": start_block}
        if end_block:
            find["timeframe.end.block"] = {"$lte": end_block}

        # find reports
        return await local_database_helper(network=chain).get_items_from_database(
            collection_name="reports",
            find=find,
        )

    async def get_gross_fees_ramses(
        self,
        response: Response,
    ):
        # return a sorted by period list of gross fees
        return sorted(
            [
                x["data"]
                for x in await self.get_report(chain=Chain.ARBITRUM, response=response)
            ],
            key=lambda x: x["period"],
        )
