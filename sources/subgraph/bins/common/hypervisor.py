import asyncio
import logging
from datetime import datetime, timedelta

from fastapi import Response, status
from gql.transport.exceptions import TransportQueryError

from sources.subgraph.bins.common import ExecutionOrderWrapper
from sources.subgraph.bins.common.hypervisors.all_data import AllData as HypeAllData
from sources.subgraph.bins.common.hypervisors.basic_stats import BasicStats
from sources.subgraph.bins.config import MONGO_DB_URL
from sources.subgraph.bins.database.managers import (
    db_allData_manager,
    db_returns_manager,
    db_unifiedData_manager,
)
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees import fees_all
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.hype_fees.impermanent_divergence import (
    impermanent_divergence_all,
)
from sources.subgraph.bins.hypervisor import HypervisorData
from sources.subgraph.bins.toplevel import TopLevelData

logger = logging.getLogger(__name__)


class HypeBasicStats(ExecutionOrderWrapper):
    async def _database(self):
        _mngr = db_allData_manager(mongo_url=MONGO_DB_URL)
        result = await _mngr.get_data(chain=self.chain, protocol=self.protocol)
        self.database_datetime = result.pop("datetime", "")
        for hype in result.values():
            hype.pop("returns")
        return result

    async def _subgraph(self):
        basic_stats = BasicStats(chain=self.chain, protocol=self.protocol)
        return await basic_stats.basic_stats()


class AllData(ExecutionOrderWrapper):
    async def _database(self):
        _mngr = db_allData_manager(mongo_url=MONGO_DB_URL)
        result = await _mngr.get_data(chain=self.chain, protocol=self.protocol)
        self.database_datetime = result.pop("datetime", "")
        return result

    async def _subgraph(self):
        hype_all_data = HypeAllData(chain=self.chain, protocol=self.protocol)
        return await hype_all_data.all_data()


class FeeReturns(ExecutionOrderWrapper):
    def __init__(
        self,
        protocol: Protocol,
        chain: Chain,
        days: int,
        current_timestamp: int | None = None,
        response: Response = None,
    ):
        self.days = days
        self.current_timestamp = current_timestamp
        super().__init__(protocol, chain, response)

    async def _database(self):
        returns_manager = db_returns_manager(mongo_url=MONGO_DB_URL)
        result = await returns_manager.get_feeReturns(
            chain=self.chain, protocol=self.protocol, period=self.days
        )
        self.database_datetime = result.pop("datetime", "")
        return result

    async def _subgraph(self):
        return (
            await fee_returns_all(
                protocol=self.protocol,
                chain=self.chain,
                days=self.days,
                current_timestamp=self.current_timestamp,
            )
        )["lp"]


class HypervisorsReturnsAllPeriods(ExecutionOrderWrapper):
    def __init__(
        self,
        protocol: Protocol,
        chain: Chain,
        hypervisors: list[str] | None = None,
        current_timestamp: int | None = None,
        response: Response = None,
    ):
        self.hypervisors = (
            [hypervisor.lower() for hypervisor in hypervisors] if hypervisors else None
        )
        self.current_timestamp = current_timestamp
        super().__init__(protocol, chain, response)

    async def _database(self):
        average_returns_mngr = db_returns_manager(mongo_url=MONGO_DB_URL)

        av_result = await average_returns_mngr.get_hypervisors_returns_average(
            chain=self.chain, protocol=self.protocol
        )
        if len(av_result) < 0:
            raise ValueError(" No returns")

        results_na = {"feeApr": 0, "feeApy": 0, "status": "unavailable on database"}

        result = {}
        # CONVERT result so is equal to original
        for hypervisor in av_result:
            result[hypervisor["_id"]] = {}
            try:
                result[hypervisor["_id"]]["daily"] = {
                    "feeApr": hypervisor["returns"]["1"]["av_feeApr"],
                    "feeApy": hypervisor["returns"]["1"]["av_feeApy"],
                    "status": "database",
                }
            except Exception:
                result[hypervisor["_id"]]["daily"] = results_na
            try:
                result[hypervisor["_id"]]["weekly"] = {
                    "feeApr": hypervisor["returns"]["7"]["av_feeApr"],
                    "feeApy": hypervisor["returns"]["7"]["av_feeApy"],
                    "status": "database",
                }
            except Exception:
                result[hypervisor["_id"]]["weekly"] = results_na
            try:
                result[hypervisor["_id"]]["monthly"] = {
                    "feeApr": hypervisor["returns"]["30"]["av_feeApr"],
                    "feeApy": hypervisor["returns"]["30"]["av_feeApy"],
                    "status": "database",
                }
            except Exception:
                result[hypervisor["_id"]]["monthly"] = results_na
            try:
                result[hypervisor["_id"]]["allTime"] = {
                    "feeApr": hypervisor["returns"]["30"]["av_feeApr"],
                    "feeApy": hypervisor["returns"]["30"]["av_feeApy"],
                    "status": "database",
                }
            except Exception:
                result[hypervisor["_id"]]["allTime"] = results_na

        return result

    async def _subgraph(self):
        daily, weekly, monthly = await asyncio.gather(
            fee_returns_all(
                self.protocol, self.chain, 1, self.hypervisors, self.current_timestamp
            ),
            fee_returns_all(
                self.protocol, self.chain, 7, self.hypervisors, self.current_timestamp
            ),
            fee_returns_all(
                self.protocol, self.chain, 30, self.hypervisors, self.current_timestamp
            ),
        )

        results = {}
        for hypervisor_id in daily.get("lp", {}).keys():
            hypervisor_daily = daily["lp"].get(hypervisor_id)
            hypervisor_weekly = weekly["lp"].get(hypervisor_id)
            hypervisor_monthly = monthly["lp"].get(hypervisor_id)

            if hypervisor_daily:
                symbol = hypervisor_daily.pop("symbol")
            if hypervisor_weekly:
                hypervisor_weekly.pop("symbol")
            if hypervisor_monthly:
                hypervisor_monthly.pop("symbol")

            if hypervisor_weekly["feeApr"] == 0:
                hypervisor_weekly = hypervisor_daily

            if hypervisor_monthly["feeApr"] == 0:
                hypervisor_monthly = hypervisor_weekly

            results[hypervisor_id] = {
                "symbol": symbol,
                "daily": hypervisor_daily,
                "weekly": hypervisor_weekly,
                "monthly": hypervisor_monthly,
                "allTime": hypervisor_monthly,
            }

        return results


class ImpermanentDivergence(ExecutionOrderWrapper):
    def __init__(
        self,
        protocol: Protocol,
        chain: Chain,
        days: int,
        current_timestamp: int | None = None,
        response: Response = None,
    ):
        self.days = days
        self.current_timestamp = current_timestamp
        super().__init__(protocol, chain, response)

    async def _database(self):
        # check days in database
        if self.days not in [1, 7, 30]:
            raise NotImplementedError("Requested period does not exist in database")
        returns_mngr = db_returns_manager(mongo_url=MONGO_DB_URL)
        return await returns_mngr.get_impermanentDivergence_data(
            chain=self.chain, protocol=self.protocol, period=self.days
        )

    async def _subgraph(self):
        return await impermanent_divergence_all(
            protocol=self.protocol,
            chain=self.chain,
            days=self.days,
            current_timestamp=self.current_timestamp,
        )


async def hypervisor_basic_stats(
    protocol: Protocol, chain: Chain, hypervisor_address: str, response: Response
):
    all_data = HypeAllData(chain=chain, protocol=protocol)
    try:
        return await all_data.basic_stats(hypervisor_address)
    except (KeyError, TransportQueryError):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return "Invalid hypervisor address or not enough data"


async def recent_fees(protocol: Protocol, chain: Chain, hours: int = 24):
    top_level = TopLevelData(protocol, chain)
    recent_fees = await top_level.recent_fees(hours)

    return {"periodHours": hours, "fees": recent_fees}


async def hypervisors_average_return(
    protocol: Protocol, chain: Chain, response: Response = None
):
    if response:
        response.headers["X-Database"] = "true"
    average_returns_mngr = db_returns_manager(mongo_url=MONGO_DB_URL)
    return await average_returns_mngr.get_hypervisors_average(
        chain=chain, protocol=protocol
    )


async def hypervisor_average_return(
    protocol: Protocol, chain: Chain, hypervisor_address: str, response: Response = None
):
    if response:
        response.headers["X-Database"] = "true"
    average_returns_mngr = db_returns_manager(mongo_url=MONGO_DB_URL)
    return await average_returns_mngr.get_hypervisor_average(
        chain=chain, hypervisor_address=hypervisor_address, protocol=protocol
    )


async def uncollected_fees(
    protocol: Protocol,
    chain: Chain,
    hypervisor_address: str,
    current_timestamp: int | None = None,
    response: Response = None,
):
    if response:
        response.headers["X-Database"] = "false"
    return await fees_all(
        protocol=protocol,
        chain=chain,
        hypervisors=[hypervisor_address],
        current_timestamp=current_timestamp,
    )


async def uncollected_fees_all(
    protocol: Protocol, chain: Chain, current_timestamp: int | None = None
):
    return await fees_all(
        protocol=protocol, chain=chain, current_timestamp=current_timestamp
    )


async def collected_fees(
    protocol: Protocol,
    chain: Chain,
    start_timestamp: int | None = None,
    end_timestamp: int | None = None,
    start_block: int | None = None,
    end_block: int | None = None,
    usd_total_only: bool = False,
) -> dict:
    """Collected fees

    Args:
        protocol (Protocol):
        chain (Chain):
        start_timestamp (int | None, optional): . Defaults to None.
        end_timestamp (int | None, optional): . Defaults to None.
        start_block (int | None, optional): . Defaults to None.
        end_block (int | None, optional): . Defaults to None.
        usd_total_only (bool, optional): return the sum of all period_grossFeesClaimed in usd. Defaults to False.

    Returns:
        dict:
    """
    if (not start_timestamp and not start_block) or (
        not end_timestamp and not end_block
    ):
        current_month_first_day: datetime = datetime.utcnow().replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        last_month_first_day: datetime = (
            current_month_first_day - timedelta(days=current_month_first_day.day)
        ).replace(day=1)

        start_date = last_month_first_day
        end_date = (start_date + timedelta(days=33)).replace(
            day=1, hour=0, minute=0, second=0
        )
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

    hype = HypervisorData(protocol=protocol, chain=chain)
    collected_fees = await hype._get_collected_fees(
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        start_block=start_block,
        end_block=end_block,
    )
    if not usd_total_only or not collected_fees:
        return collected_fees
    first_key = next(iter(collected_fees))
    initial_grossFeesClaimedUSD = 0
    end_grossFeesClaimedUSD = 0
    period_grossFeesClaimedUSD = 0
    for k, x in collected_fees.items():
        initial_grossFeesClaimedUSD += x["initialGrossFeesClaimedUsd"]
        end_grossFeesClaimedUSD += x["endGrossFeesClaimedUsd"]
        period_grossFeesClaimedUSD += x["periodGrossFeesClaimedUsd"]

    return {
        "initialBlock": collected_fees[first_key]["initialBlock"],
        "initialTimestamp": collected_fees[first_key]["initialTimestamp"],
        "initialDatetime": datetime.fromtimestamp(
            collected_fees[first_key]["initialTimestamp"]
        ),
        "endBlock": collected_fees[first_key]["endBlock"],
        "endTimestamp": collected_fees[first_key]["endTimestamp"],
        "endDatetime": datetime.fromtimestamp(
            collected_fees[first_key]["endTimestamp"]
        ),
        "initialGrossFeesClaimedUSD": initial_grossFeesClaimedUSD,
        "endGrossFeesClaimedUSD": end_grossFeesClaimedUSD,
        "periodGrossFeesClaimedUSD": period_grossFeesClaimedUSD,
    }


async def unified_hypervisors_data(
    chain: Chain | None = None, protocol: Protocol | None = None
) -> list:
    """Only database results

    Returns:
        list:
    """
    return await db_unifiedData_manager(mongo_url=MONGO_DB_URL).get_data(
        chain=chain, protocol=protocol
    )
