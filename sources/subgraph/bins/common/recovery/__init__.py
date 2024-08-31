from gql.dsl import DSLQuery

from sources.subgraph.bins.common.recovery.schema import (
    RecoveryDistribution,
    RecoveryOutput,
)
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.recovery_pool import RecoveryPoolClient
from sources.subgraph.bins.utils import timestamp_to_date


class RecoveryInfo(SubgraphData):
    def __init__(self, tokenAddress):
        super().__init__()
        self.data: {}
        self.token = tokenAddress
        self.client = RecoveryPoolClient()

    def query(self, days: int, timezone: str):
        ds = self.client.data_schema

        return DSLQuery(
            ds.Query.dailyDistributions(
                first=days,
                where={
                    "token": self.token,
                    "timezone": timezone,
                },
                orderBy="date",
                orderDirection="desc",
            ).select(
                ds.DailyDistribution.date,
                ds.DailyDistribution.distributed,
                ds.DailyDistribution.cumulativeDistributed,
            ),
            ds.Query.token(id=self.token).select(ds.Token.decimals),
        )

    def _transform_data(self) -> RecoveryOutput:
        decimals = self.query_response["token"]["decimals"]
        dailyDistributions = [
            RecoveryDistribution(
                timestamp=int(day["date"]),
                date=timestamp_to_date(int(day["date"]), format="%m/%d/%Y"),
                feesEarned=int(day["distributed"]) / 10**decimals,
                usdcRecovered=int(day["distributed"]) / 10**decimals,
                usdcRecoveredCumulative=int(day["cumulativeDistributed"])
                / 10**decimals,
            )
            for day in self.query_response["dailyDistributions"]
        ]

        return RecoveryOutput(
            dailyDistributions=dailyDistributions, latest=dailyDistributions[0]
        )


async def recovery_stats(days: int, timezone: str) -> RecoveryOutput:
    recovery_info = RecoveryInfo(
        tokenAddress="0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    )
    await recovery_info.get_data(days=days, timezone=timezone)

    return recovery_info.data
