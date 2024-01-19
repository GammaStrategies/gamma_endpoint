import calendar
from datetime import datetime, timezone
from sources.common.general.enums import Chain, Protocol
from sources.mongo.bins.helpers import global_database_helper


async def get_revenue_stats(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    yearly: bool = False,
    filter_zero_revenue: bool = False,
    trailing: int = 30,
) -> list[dict]:
    """Returns the Gamma's Volume, fees and revenue

    Args:
        chain (Chain, optional): chain. Defaults to None.
        protocol (Protocol, optional): protocol. Defaults to None.
        ini_timestamp (int, optional): initial timestamp. Defaults to None.
        yearly (bool, optional): group by year. Defaults to False.
        filter_zero_revenue (bool, optional): filter out zero revenue items. Defaults to True.
        trailing (int, optional): trailing days [ only functionalt when yearly is]. Defaults to 0.

    Returns:
        dict: revenue status

    """
    result = await global_database_helper().get_items_from_database(
        collection_name="frontend",
        aggregate=(
            query_frontend_revenue_stats_by_monthDex(
                chain=chain,
                protocol=protocol,
                ini_timestamp=ini_timestamp,
                filter_zero_revenue=filter_zero_revenue,
            )
            if not yearly
            else query_frontend_revenue_stats_byYear(
                chain=chain,
                protocol=protocol,
                ini_timestamp=ini_timestamp,
                filter_zero_revenue=filter_zero_revenue,
            )
        ),
    )

    # sort by year
    if yearly:
        result.sort(key=lambda x: x["year"])

        # add a monthly estimation using trailing days
        trailing_usd = 0
        try:
            _months_back = trailing // 30
            # define current month as last item (year) -> last 'items' (month)
            _current_month = result[-1]["items"][-1]
            # define first month as current month minus trailing months
            _first_month = _current_month["month"] - _months_back

            if _current_month["month"] == 1:
                _last_month = result[-2]["items"][-1]
            else:
                _last_month = result[-1]["items"][-2]

            # get last_month total days
            _last_month["total_revenue"] / 30

            # get current_month total days left
            days_passed_current_month = datetime.now(timezone.utc).day
            total_days_current_month = calendar.monthrange(
                datetime.now(timezone.utc).year, datetime.now(timezone.utc).month
            )[1]
            days_left_current_month = (
                total_days_current_month - days_passed_current_month
            )

            trailing_usd = (
                (_last_month["total_revenue"] / 30) * days_left_current_month
            ) + _current_month["total_revenue"]

            # add it to the current year
            result[-1]["total_revenue_potential"] = trailing_usd * 12

        except Exception as e:
            pass

    return result


def query_frontend_revenue_stats_by_monthDex(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    filter_zero_revenue: bool = False,
) -> list[dict]:
    """Frontent collection query for revenue stats ordered by month and dex

    Args:
        chain (Chain, optional): chain. Defaults to None.
        protocol (Protocol, optional): protocol. Defaults to None.
        ini_timestamp (int, optional): initial timestamp. Defaults to None.
        filter_zero_revenue (bool, optional): filter out zero revenue. Defaults to True.

    Returns:
        list[dict]: query pipeline

    """
    # fix frontend type
    _match = {"frontend_type": "revenue_stats"}

    # filter by chain, protocol and timestamp
    if chain:
        _match["chain"] = chain.database_name
    if protocol:
        _match["protocol"] = protocol.database_name
    if ini_timestamp:
        _match["timestamp"] = {"$gte": ini_timestamp}
    if filter_zero_revenue:
        _match["total_revenue"] = {"$gt": 0}

    # build query
    _query = [
        {"$match": _match},
        {"$sort": {"timestamp": 1, "total_revenue": -1}},
        {
            "$group": {
                "_id": "$timestamp",
                "timestamp": {"$first": "$timestamp"},
                "datetime": {
                    "$first": {"$toDate": {"$multiply": ["$timestamp", 1000]}}
                },
                "total_revenue": {"$sum": "$total_revenue"},
                "total_fees": {"$sum": "$total_fees"},
                "total_volume": {"$sum": "$total_volume"},
                "items": {
                    "$push": {
                        "chain": "$chain",
                        "protocol": "$protocol",
                        "chain_id": "$chain_id",
                        "timestamp": "$timestamp",
                        "total_revenue": "$total_revenue",
                        "total_fees": "$total_fees",
                        "total_volume": "$total_volume",
                        "exchange": "$exchange",
                    }
                },
            }
        },
        {
            "$addFields": {
                "year": {"$year": "$datetime"},
                "month": {"$month": "$datetime"},
            }
        },
        {"$unset": ["_id", "id"]},
        {"$sort": {"timestamp": 1, "total_revenue": -1}},
    ]

    return _query


def query_frontend_revenue_stats_byYear(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    filter_zero_revenue: bool = False,
):
    """Frontent collection query for revenue stats grouped by year

    Args:
        chain (Chain, optional): chain. Defaults to None.
        protocol (Protocol, optional): protocol. Defaults to None.
        ini_timestamp (int, optional): initial timestamp. Defaults to None.
        filter_zero_revenue (bool, optional): filter out zero revenue items. Defaults to True.

    Returns:
        list[dict]: query pipeline
    """
    # use monthly
    _query = query_frontend_revenue_stats_by_monthDex(
        chain=chain,
        protocol=protocol,
        ini_timestamp=ini_timestamp,
        filter_zero_revenue=filter_zero_revenue,
    )
    #  group by year
    _query.append(
        {
            "$group": {
                "_id": "$year",
                "year": {"$first": "$year"},
                "total_fees": {"$sum": "$total_fees"},
                "total_revenue": {"$sum": "$total_revenue"},
                "total_volume": {"$sum": "$total_volume"},
                "items": {"$push": "$$ROOT"},
            }
        }
    )
    _query.append({"$unset": "_id"})
    return _query
