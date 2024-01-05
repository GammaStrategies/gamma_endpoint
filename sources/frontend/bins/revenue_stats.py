from sources.common.general.enums import Chain, Protocol
from sources.mongo.bins.helpers import global_database_helper


async def get_revenue_stats(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
    yearly: bool = False,
) -> list[dict]:
    """Returns the Gamma's Volume, fees and revenue

    Returns:
        dict: revenue status

    """
    result = await global_database_helper().get_items_from_database(
        collection_name="frontend",
        aggregate=(
            query_frontend_revenue_stats_by_monthDex(
                chain=chain, protocol=protocol, ini_timestamp=ini_timestamp
            )
            if not yearly
            else query_frontend_revenue_stats_byYear(
                chain=chain, protocol=protocol, ini_timestamp=ini_timestamp
            )
        ),
    )

    return result


def query_frontend_revenue_stats_by_monthDex(
    chain: Chain | None = None,
    protocol: Protocol | None = None,
    ini_timestamp: int | None = None,
) -> list[dict]:
    """Frontent collection query for revenue stats ordered by month and dex

    Returns:
        list[dict]: query pipeline

    """

    _match = {"frontend_type": "revenue_stats"}
    if chain:
        _match["chain"] = chain.database_name
    if protocol:
        _match["protocol"] = protocol.database_name
    if ini_timestamp:
        _match["timestamp"] = {"$gte": ini_timestamp}

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
):
    # use monthly
    _query = query_frontend_revenue_stats_by_monthDex(
        chain=chain, protocol=protocol, ini_timestamp=ini_timestamp
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
