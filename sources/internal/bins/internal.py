"""Internal Endpoints"""

from pydantic import BaseModel


class InternalFeeYield(BaseModel):
    """APR and APY in allData"""

    totalApr: float = 0
    totalApy: float = 0
    lpApr: float = 0
    lpApy: float = 0
    status: str = "No data"


class InternalFeeReturnsOutput(BaseModel):
    """Output model for internal fee returns"""

    symbol: str
    daily: InternalFeeYield = InternalFeeYield()
    weekly: InternalFeeYield = InternalFeeYield()
    monthly: InternalFeeYield = InternalFeeYield()


class InternalTokens(BaseModel):
    token0: float = 0
    token1: float = 0
    usd: float = 0


class InternalTimeframe(BaseModel):
    ini: int = 0
    end: int = 0


class InternalKpi(BaseModel):
    gamma_vs_pool_liquidity_ini: float = 0
    gamma_vs_pool_liquidity_end: float = 0

    feeTier: float = 0  # percentage
    eVolume: float = 0  # estimated volume in usd


class InternalGrossFeesOutput(BaseModel):
    """Output model for internal gross fees qtty in a period"""

    symbol: str
    days_period: int

    block: InternalTimeframe = InternalTimeframe()
    timestamp: InternalTimeframe = InternalTimeframe()

    deposits: InternalTokens = InternalTokens()
    withdraws: InternalTokens = InternalTokens()

    collectedFees: InternalTokens = InternalTokens()
    uncollected: InternalTokens = InternalTokens()

    protocolFee_0: int = 0
    protocolFee_1: int = 0

    calculatedGrossFees: InternalTokens = InternalTokens()

    measurements: InternalKpi = InternalKpi()
