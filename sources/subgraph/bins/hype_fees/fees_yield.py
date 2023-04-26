import logging

import numpy as np
from pandas import DataFrame

from sources.subgraph.bins.constants import DAY_SECONDS, YEAR_SECONDS
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.data import FeeGrowthSnapshotData
from sources.subgraph.bins.hype_fees.fees import Fees
from sources.subgraph.bins.hype_fees.schema import FeesData, FeesSnapshot, FeeYield

logger = logging.getLogger(__name__)

YIELD_PER_DAY_MAX = 300


class FeesYield:
    def __init__(self, data: list[FeesData], protocol: Protocol, chain: Chain) -> None:
        self.data = data
        self.protocol = protocol
        self.chain = chain

    def calculate_returns(self, apr_type: str | None = None) -> FeeYield:
        """Calculate APR and APY for fees

        Args:
            apr_type (str, optional): users:  use Liquiditiy Provider fees to calculate feeApr,
                                     gamma:  use Gamma's fees to calculate feeApr,
                                     all:    use totalFees ( lp's+gammas) to calculate feeApr

        Returns:
            FeeYield:
        """
        # default apr typeis Liquidity Providers feeApr (users)
        apr_type = apr_type or "users"

        snapshots = [self.get_fees(entry) for entry in self.data]
        df_snapshots = DataFrame(snapshots, dtype=np.float64)

        #  Require at least two rows to calculate yield
        if len(df_snapshots) < 2:
            logger.info("No hypervisor data - skipping calculations")
            return FeeYield(
                apr=0,
                apy=0,
                status="Insufficient Data",
            )

        df_snapshots = df_snapshots.set_index("block").sort_index()
        # fee % is 1 / fee or 1/10 if fee > 100
        df_snapshots["fee_calc"] = df_snapshots["fee"].apply(
            lambda x: 1 / x if x < 100 else 1 / 10
        )
        df_snapshots["gamma_fee_0"] = df_snapshots.total_fees_0 * df_snapshots.fee_calc
        df_snapshots["gamma_fee_1"] = df_snapshots.total_fees_1 * df_snapshots.fee_calc
        df_snapshots["lp_fee_0"] = df_snapshots.total_fees_0 - df_snapshots.gamma_fee_0
        df_snapshots["lp_fee_1"] = df_snapshots.total_fees_1 - df_snapshots.gamma_fee_1

        df_snapshots["elapsed_time"] = df_snapshots.timestamp.diff()

        # Choose fee growth based on aprType
        if apr_type == "users":
            df_snapshots["fee0_growth"] = df_snapshots.lp_fee_0.diff().clip(lower=0)
            df_snapshots["fee1_growth"] = df_snapshots.lp_fee_1.diff().clip(lower=0)
        elif apr_type == "gamma":
            df_snapshots["fee0_growth"] = df_snapshots.gamma_fee_0.diff().clip(lower=0)
            df_snapshots["fee1_growth"] = df_snapshots.gamma_fee_1.diff().clip(lower=0)
        elif apr_type == "all":
            df_snapshots["fee0_growth"] = df_snapshots.total_fees_0.diff().clip(lower=0)
            df_snapshots["fee1_growth"] = df_snapshots.total_fees_1.diff().clip(lower=0)

        df_snapshots["fee_growth_usd"] = (
            df_snapshots.fee0_growth * df_snapshots.price_0
            + df_snapshots.fee1_growth * df_snapshots.price_1
        )
        df_snapshots["period_yield"] = (
            df_snapshots.fee_growth_usd / df_snapshots.tvl_usd
        )
        df_snapshots["yield_per_day"] = (
            df_snapshots.period_yield * YEAR_SECONDS / df_snapshots.elapsed_time
        )

        has_outlier = (df_snapshots.yield_per_day > YIELD_PER_DAY_MAX).any()
        df_snapshots = df_snapshots[df_snapshots.yield_per_day < YIELD_PER_DAY_MAX]

        df_snapshots["total_period_seconds"] = df_snapshots.elapsed_time.cumsum()
        df_snapshots["cum_fee_return"] = (1 + df_snapshots.period_yield).cumprod() - 1

        df_returns = df_snapshots[["total_period_seconds", "cum_fee_return"]].tail(1)

        # This is a failsafe for if there are outliers
        if df_returns.empty:
            logger.debug("Empty returns")
            return FeeYield(
                apr=0,
                apy=0,
                status="Insufficient good data",
            )

        # Extrapolate linearly to annual rate
        df_returns["fee_apr"] = df_returns.cum_fee_return * (
            YEAR_SECONDS / df_returns.total_period_seconds
        )

        # Extrapolate by compounding
        df_returns["fee_apy"] = (
            1
            + df_returns.cum_fee_return
            * (DAY_SECONDS / df_returns.total_period_seconds)
        ) ** 365 - 1

        df_returns = df_returns.fillna(0).replace({np.inf: 0, -np.inf: 0})

        returns = df_returns.to_dict("records")[0]

        returns["fee_apr"] = max(returns["fee_apr"], 0)
        returns["fee_apy"] = max(returns["fee_apy"], 0)

        return FeeYield(
            apr=returns["fee_apr"] if returns["fee_apr"] else 0,
            apy=returns["fee_apy"] if returns["fee_apy"] else 0,
            status="Outlier removed" if has_outlier else "Good",
        )

    def get_fees(self, fees_data: FeesData) -> FeesSnapshot:
        fees = Fees(fees_data, self.protocol, self.chain)
        fee_amounts = fees.fee_amounts()

        return FeesSnapshot(
            block=fees_data.block,
            timestamp=fees_data.timestamp,
            fee=fees_data.fee,
            tvl_usd=fees_data.tvl_usd,
            total_fees_0=fee_amounts.total.amount.value0,
            total_fees_1=fee_amounts.total.amount.value1,
            price_0=fees_data.price.value0,
            price_1=fees_data.price.value1,
        )


async def fee_returns_all(
    protocol: Protocol,
    chain: Chain,
    days: int,
    hypervisors: list[str] | None = None,
    current_timestamp: int | None = None,
    apr_type: str | None = None,
) -> dict[str, dict]:
    fees_data = FeeGrowthSnapshotData(protocol, chain)
    await fees_data.init_time(days_ago=days, end_timestamp=current_timestamp)
    await fees_data.get_data(hypervisors)

    results = {}
    for hypervisor_id, fees_data in fees_data.data.items():
        fees_yield = FeesYield(fees_data, protocol, chain)

        returns = (
            fees_yield.calculate_returns(apr_type=apr_type)
            if apr_type
            else fees_yield.calculate_returns()
        )
        results[hypervisor_id] = {
            "symbol": fees_data[0].symbol,
            "feeApr": returns.apr,
            "feeApy": returns.apy,
            "status": returns.status,
        }
    return results
