from pydantic import BaseModel

class HypervisorBasicInfoOutput(BaseModel):
    """Output schema for Hypervisor Basic Stats"""

    createDate: str
    poolAddress: str
    name: str
    token0: str
    token1: str
    decimals0: int
    decimals1: int
    depositCap0: float
    depositCap1: float
    grossFeesClaimed0: float
    grossFeesClaimed1: float
    grossFeesClaimedUSD: str
    feesReinvested0: float
    feesReinvested1: float
    feesReinvestedUSD: str
    tvl0: float
    tvl1: float
    tvlUSD: str
    totalSupply: int
    maxTotalSupply: int
    capacityUsed: str
    sqrtPrice: str
    tick: int
    baseLower: int
    baseUpper: int
    inRange: bool
    observationIndex: str
    poolTvlUSD: str
    poolFeesUSD: str

class AllDataReturnsYield(BaseModel):
    feeApr: float
    feeApy: float


class AllDataReturns(BaseModel):
    daily: AllDataReturnsYield
    weekly: AllDataReturnsYield
    monthly: AllDataReturnsYield
    allTime: AllDataReturnsYield
    status: str


class AllDataOutput(HypervisorBasicInfoOutput):
    returns: AllDataReturns
