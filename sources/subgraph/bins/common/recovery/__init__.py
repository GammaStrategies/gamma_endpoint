from sources.subgraph.bins.common.recovery.schema import (
    RecoveryDistribution,
    RecoveryOutput,
)


async def recovery_stats() -> RecoveryOutput:
    return RecoveryOutput(
        dailyDistributions=[
            RecoveryDistribution(
                timestamp=1705986000,
                date="01/24/2024",
                feesEarned=823.2,
                usdcRecovered=678.1,
                usdcRecoveredCumulative=1980.6,
            ),
            RecoveryDistribution(
                timestamp=1705813200,
                date="01/21/2024",
                feesEarned=683.1,
                usdcRecovered=456.3,
                usdcRecoveredCumulative=1524.3,
            ),
        ]
    )
