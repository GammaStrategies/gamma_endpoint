import asyncio
import time
from sources.common.general.utils import convert_to_csv
from sources.common.prices.helpers import get_database_prices_closeto
from sources.internal.bins.fee_internal import (
    get_chain_usd_fees,
    get_revenue_operations,
)
from sources.internal.bins.kpis import (
    get_average_tvl,
    get_transactions,
    get_transactions_summary,
    get_users_activity,
)
from sources.subgraph.bins.enums import Chain, Protocol
from sources.mongo.bins.helpers import local_database_helper, global_database_helper


async def report_galaxe(
    chain: Chain,
    user_address: str | None = None,
    net_position_usd_threshold: int | None = None,
    deposits_usd_threshold: int | None = None,
):
    """Custom report for Galaxe

    Args:
        chain (Chain):
        net_position_usd_threshold (int | None, optional): . Defaults to any.
        deposits_usd_threshold (int | None, optional): . Defaults to any.

    """
    full_report = await local_database_helper(network=chain).get_items_from_database(
        collection_name="reports",
        find={"id": f"usersActivity_{chain.database_name}_users_net_position_Galxe"},
        projection={"_id": 0},
    )
    full_report = full_report[0]
    full_report.pop("id")

    result = {"user_addresses": [], "details": {}}
    for user, user_details in full_report.items():
        if user_address is not None:
            if user_address != user:
                continue

        if net_position_usd_threshold is not None:
            if user_details["total_net_position"]["usd"] >= net_position_usd_threshold:
                result["user_addresses"].append(user)
                result["details"][user] = user_details
        elif deposits_usd_threshold is not None:
            if user_details["total_deposits"]["usd"] >= deposits_usd_threshold:
                result["user_addresses"].append(user)
                result["details"][user] = user_details
        else:
            result["user_addresses"].append(user)
            result["details"][user] = user_details

    return result


async def global_report_revenue():
    """Global report revenue

    Returns:
        dict: _description_

    """
    result = await global_database_helper().get_items_from_database(
        collection_name="reports",
        find={"id": "revenue"},
        projection={"_id": 0, "id": 0},
    )
    return result[0]


async def custom_report(chain: Chain, user_address: str | None = None):
    """Custom report

    Returns:
        dict: _description_

    """
    find = {"type": "custom"}
    if user_address is not None:
        find["user_address"] = user_address
    data = await local_database_helper(network=chain).get_items_from_database(
        collection_name="reports",
        find=find,
        projection={"_id": 0, "id": 0},
    )

    return [global_database_helper().convert_d128_to_decimal(x) for x in data]


## KPIs


async def get_kpis_dashboard(
    chain: Chain,
    protocol: Protocol,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    period_seconds: int | None = None,
    hypervisor_addresses: list[str] | None = None,
):

    # create a list of ini_timestamp,end_timestamp tuples for each period
    periods = []
    if period_seconds:
        # calculate the periods
        _tmp_end_timestamp = end_timestamp or int(time.time())
        while _tmp_end_timestamp > ini_timestamp:
            periods.append((_tmp_end_timestamp - period_seconds, _tmp_end_timestamp))
            _tmp_end_timestamp -= period_seconds

    else:
        periods.append((ini_timestamp, end_timestamp))

    result = []
    # reverse the periods
    periods = periods[::-1]
    for ini_time, end_time in periods:

        # get prices close to the period ( to be used in the calculations)
        _prices = await get_database_prices_closeto(
            chain=chain,
            timestamp=end_time,
            default_to_current=True,
        )

        # Average TVL	Δ% TVL	Fees	∑ Fees	Fees / Day	Revenue	∑ Revenue	Revenue / Day	Volume	∑ Volume	Volume / Day	Cap Efficiency 	Fee APR	Incentives	∑ Incentives	Incentives / Day	Avg Price	Incentives ($)	∑ Incentives ($)	Incentives / Day ($)	Incentive APR	Incentivized LR	Deposits	Withdrawals	Compounds	Rebalances	Users	Total Txns	∑ Txns	Txns / Day	D/W Ratio
        average_tvl, transactions_summary, user_activity, transactions = (
            await asyncio.gather(
                get_average_tvl(
                    chain=chain,
                    protocol=protocol,
                    ini_timestamp=ini_time,
                    end_timestamp=end_time,
                    hypervisors=hypervisor_addresses,
                    prices=_prices,
                ),
                get_transactions_summary(
                    chain=chain,
                    protocol=protocol,
                    ini_timestamp=ini_time,
                    end_timestamp=end_time,
                    hypervisors=hypervisor_addresses,
                    prices=_prices,
                ),
                get_users_activity(
                    chain=chain,
                    ini_timestamp=ini_time,
                    end_timestamp=end_time,
                    hypervisors=hypervisor_addresses,
                ),
                get_transactions(
                    chain=chain,
                    protocol=protocol,
                    ini_timestamp=ini_time,
                    end_timestamp=end_time,
                    hypervisors=hypervisor_addresses,
                    prices=_prices,
                ),
            )
        )

        transactions = (
            transactions.get(chain.id, {})
            or transactions.get(str(chain.id), {})
            or {
                "deposits_qtty": 0,
                "withdraws_qtty": 0,
                "zeroBurns_qtty": 0,
                "rebalances_qtty": 0,
                "transfers_qtty": 0,
            }
        )

        # append the data
        result.append(
            {
                "ini_timestamp": average_tvl["ini_timestamp"],
                "end_timestamp": average_tvl["end_timestamp"],
                "average_tvl": average_tvl["average_tvl"],
                "fees": transactions_summary["fees_usd"],
                "gross_fees": transactions_summary["gross_fees_usd"],
                "volume": transactions_summary["volume"],
                "deposits": transactions["deposits_qtty"],
                "withdraws": transactions["withdraws_qtty"],
                "compounds": transactions["zeroBurns_qtty"],
                "rebalances": transactions["rebalances_qtty"],
                "transfers": transactions["transfers_qtty"],
                "users": user_activity["total_users"],
            }
        )

    # convert to csv
    csv_result = convert_to_csv(result)

    return csv_result
