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
