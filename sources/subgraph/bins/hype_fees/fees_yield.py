import logging

import numpy as np
from pandas import DataFrame

from sources.common.formulas.fees import calculate_gamma_fee
from sources.subgraph.bins.constants import DAY_SECONDS, YEAR_SECONDS
from sources.subgraph.bins.enums import Chain, Protocol, YieldType
from sources.subgraph.bins.hype_fees.data import FeeGrowthSnapshotData
from sources.subgraph.bins.hype_fees.fees import Fees
from sources.subgraph.bins.hype_fees.schema import FeesData, FeesSnapshot, FeeYield

logger = logging.getLogger(__name__)

YIELD_PER_DAY_MAX = 300


class FeesYield:
    "Calculations for fee related yields."

    def __init__(self, data: list[FeesData], protocol: Protocol, chain: Chain) -> None:
        self.data = data
        self.protocol = protocol
        self.chain = chain

    def calculate_returns(self, yield_type: YieldType = YieldType.LP) -> FeeYield:
        """Calculate APR and APY."""
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

        # Apply gamma fees to total fees if calculating LP returns
        if yield_type == YieldType.LP:
            # fee % is 1 / fee or 1/10 if fee > 100
            def fee_func_percentage(fee_rate):
                return calculate_gamma_fee(fee_rate, self.protocol)

            df_snapshots["gamma_fee_rate"] = df_snapshots["fee"].apply(
                fee_func_percentage
            )
            df_snapshots["effective_fees_0"] = df_snapshots.total_fees_0 * (
                1 - df_snapshots.gamma_fee_rate
            )
            df_snapshots["effective_fees_1"] = df_snapshots.total_fees_1 * (
                1 - df_snapshots.gamma_fee_rate
            )
        else:
            df_snapshots["effective_fees_0"] = df_snapshots.total_fees_0
            df_snapshots["effective_fees_1"] = df_snapshots.total_fees_1

        df_snapshots["elapsed_time"] = df_snapshots.timestamp.diff()
        df_snapshots["fee0_growth"] = df_snapshots.effective_fees_0.diff().clip(lower=0)
        df_snapshots["fee1_growth"] = df_snapshots.effective_fees_1.diff().clip(lower=0)

        df_snapshots["fee_growth_usd"] = (
            df_snapshots.fee0_growth * df_snapshots.price_0
            + df_snapshots.fee1_growth * df_snapshots.price_1
        )

        # handle divisionByZero errors
        df_snapshots["period_yield"] = df_snapshots.apply(
            lambda x: (
                np.nan if x["tvl_usd"] == 0 else x["fee_growth_usd"] / x["tvl_usd"]
            ),
            axis=1,
        )
        # df_snapshots["period_yield"] = (
        #     df_snapshots.fee_growth_usd / df_snapshots.tvl_usd
        # )
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
            apr=returns["fee_apr"] or 0,
            apy=returns["fee_apy"] or 0,
            status="Outlier removed" if has_outlier else "Good",
        )

    def get_fees(self, fees_data: FeesData) -> FeesSnapshot:
        """Get fee amounts given fees data."""
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
    return_total: bool = False,
    session = None,
) -> dict[str, dict]:
    """Get fee returns for multiple hypervisors."""
    fees_data = FeeGrowthSnapshotData(protocol, chain)
    await fees_data.init_time(days_ago=days, end_timestamp=current_timestamp)
    await fees_data.get_data(session, hypervisors)

    results = {"lp": {}, "total": {}}
    for hypervisor_id, fees_data in fees_data.data.items():
        fees_yield = FeesYield(fees_data, protocol, chain)
        lp_returns = fees_yield.calculate_returns(yield_type=YieldType.LP)

        results["lp"][hypervisor_id] = {
            "symbol": fees_data[0].symbol,
            "feeApr": lp_returns.apr,
            "feeApy": lp_returns.apy,
            "status": lp_returns.status,
        }

        if return_total:
            total_returns = fees_yield.calculate_returns(yield_type=YieldType.TOTAL)
            results["total"][hypervisor_id] = {
                "symbol": fees_data[0].symbol,
                "feeApr": total_returns.apr,
                "feeApy": total_returns.apy,
                "status": total_returns.status,
            }

    return results
