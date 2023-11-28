from sources.subgraph.bins.enums import Chain, Protocol
from sources.mongo.bins.helpers import local_database_helper


async def report_galaxe():
    full_report = await local_database_helper(
        network=Chain.ARBITRUM
    ).get_items_from_database(
        collection_name="reports",
        find={"id": "usersActivity_arbitrum_user_deposits_Galxe"},
        projection={"_id": 0},
    )
    full_report = full_report[0]
    full_report.pop("id")

    result = {"user_addresses": [], "details": {}}
    for user, user_details in full_report.items():
        if user_details["total_deposits"]["usd"] >= 100:
            result["user_addresses"].append(user)
            result["details"][user] = user_details

    return result
