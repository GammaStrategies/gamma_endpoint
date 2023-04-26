from pydantic import BaseModel

from sources.subgraph.bins.database.managers import db_aggregateStats_manager
from sources.subgraph.bins.common import ExecutionOrderWrapper
from sources.subgraph.bins.config import MONGO_DB_URL
from sources.subgraph.bins.toplevel import TopLevelData


class AggregateStatsOutput(BaseModel):
    """Aggregated data across all hypervisors"""

    totalValueLockedUSD: float
    pairCount: int
    totalFeesClaimedUSD: float

    def __add__(self, other):
        return AggregateStatsOutput(
            totalValueLockedUSD=self.totalValueLockedUSD + other.totalValueLockedUSD,
            pairCount=self.pairCount + other.pairCount,
            totalFeesClaimedUSD=self.totalFeesClaimedUSD + other.totalFeesClaimedUSD,
        )


class AggregateStatsDeploymentInfoOutput(AggregateStatsOutput):
    """Includes extra info about deployments that were considered"""

    deployments: list[str]


class AggregateStats(ExecutionOrderWrapper):
    async def _database(self):
        _mngr = db_aggregateStats_manager(mongo_url=MONGO_DB_URL)
        result = await _mngr.get_data(chain=self.chain, protocol=self.protocol)
        self.database_datetime = result.pop("datetime", "")

        return AggregateStatsOutput(
            totalValueLockedUSD=result["totalValueLockedUSD"],
            pairCount=result["paiCount"],
            totalFeesClaimedUSD=result["totalFeesClaimedUSD"],
        )

    async def _subgraph(self):
        top_level = TopLevelData(self.protocol, self.chain)

        top_level_data = await top_level.all_stats()

        return AggregateStatsOutput(
            totalValueLockedUSD=top_level_data["tvl"],
            pairCount=top_level_data["hypervisor_count"],
            totalFeesClaimedUSD=top_level_data["fees_claimed"],
        )
