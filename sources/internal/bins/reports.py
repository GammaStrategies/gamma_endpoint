from sources.subgraph.bins.enums import Chain, Protocol
from sources.mongo.bins.helpers import local_database_helper, global_database_helper


async def report_galaxe(
    user_address: str | None = None,
    net_position_usd_threshold: int | None = None,
    deposits_usd_threshold: int | None = None,
):
    """Custom report for Galaxe

    Args:
        net_position_usd_threshold (int | None, optional): _description_. Defaults to None.
        deposits_usd_threshold (int | None, optional): _description_. Defaults to None.

    """
    full_report = await local_database_helper(
        network=Chain.ARBITRUM
    ).get_items_from_database(
        collection_name="reports",
        find={"id": "usersActivity_arbitrum_users_net_position_Galxe"},
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
