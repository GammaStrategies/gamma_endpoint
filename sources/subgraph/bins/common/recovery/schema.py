from typing import List

from pydantic import BaseModel


class RecoveryDistribution(BaseModel):
    """Recovery stats for each day"""

    timestamp: int
    date: str
    feesEarned: float
    usdcRecovered: float
    usdcRecoveredCumulative: float


class RecoveryOutput(BaseModel):
    """Output for recovery"""

    dailyDistributions: List[RecoveryDistribution]
    latest: RecoveryDistribution
