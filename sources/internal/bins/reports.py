from sources.subgraph.bins.enums import Chain, Protocol
from sources.mongo.bins.helpers import local_database_helper


async def report_galaxe(
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
